import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Try



object Conll2Features {
  /* CoNLL format: for each dependency output a field with ten columns ending with the bio named entity: http://universaldependencies.org/docs/format.html
     IN_ID TOKEN LEMMA POS_COARSE POS_FULL MORPH ID_OUT TYPE _ NE_BIO
     5 books book NOUN NNS Number=Plur 2 dobj 4:dobj SpaceAfter=No */

  case class Word (lemma:String, pos:String)
  case class Feature (word:Word, dtype:String)

  val verbPos = Set("VB", "VBZ", "VBD", "VBN", "VBP", "MD")
  val verbose = false
  val conllRecordDelimiter = ">>>>>\t"
  val svoOnly = true // subject-verb-object features only
  val saveIntermediate = true
  val minSvoFreq = 5

  case class Dependency(inID: Int, inToken: String, inLemma: String, inPos: String, outID: Int, dtype: String) {
    def this(fields: Array[String]) = {
      this(fields(0).toInt, fields(1), fields(2), fields(4), fields(6).toInt, fields(7))
    }
  }

  def main(args: Array[String]) {
    if (args.size < 3) {
      println("Parameters: <input-dir> <output-dir> <verbs-only>")
      println("<input-dir>\tDirectory with a parsed corpus in the CoNLL format.'")
      println("<output-dir>\tDirectory with an output word feature files")
      println("<verbs-only>\tIf true features for verbs are saved only.")
      return
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val verbsOnly = args(2).toBoolean

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    run(sc, inputPath, outputPath, verbsOnly)
  }

  def simplifyDtype(fullDtype:String) = {
    if (fullDtype.contains("subj")) "subj"
    else if (fullDtype.contains("obj")) "obj"
    else fullDtype
  }

  def simplifyPos(fullPos:String) = {
    CoarsifyPosTags.full2coarse(fullPos)
  }

  def run(sc: SparkContext, inputConllDir: String, outputFeaturesDir: String, verbsOnly: Boolean) = {

    // Initialization
    println("Input dir.: " + inputConllDir)
    println("Output dir.: " + outputFeaturesDir)
    Util.delete(outputFeaturesDir) // a convinience for the local tests
    val conf = new Configuration
    conf.set("textinputformat.record.delimiter", conllRecordDelimiter)
    val posDepCount = sc.longAccumulator("numberOfDependenciesWithTargetPOS")
    val allDepCount = sc.longAccumulator("numberOfDependencies")
    val depErrCount = sc.longAccumulator("numberOfDependenciesWithErrors")

    // Calculate features of the individual tokens: a list of grammatical dependendies per lemma
    val unaggregatedFeatures = sc
      .newAPIHadoopFile(inputConllDir, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map { record => record._2.toString }
      .flatMap { record =>
        // parse the sentence record
        var id2dependency = collection.mutable.Map[Int, Dependency]()
        for (line <- record.split("\n")) {
          val fields = line.split("\t")
          if (fields.length == 10) {
            val inID = Try(fields(0).toInt)
            if (inID.isSuccess) {
              id2dependency(inID.get) = new Dependency(fields)
            } else {
              println(s"Warning: bad line ${line}")
            }
          } else {
            if (fields.length > 2) {
              println(s"Warning: bad line (${fields.length} fields): ${line}")
            } else {
              // the line with the original sentence: do nothing
            }
          }
        }

        // find dependent features
        val lemmas2features = collection.mutable.Map[Word, ListBuffer[Feature]]()
        for ((id, dep) <- id2dependency) {
          allDepCount.add(1)
          val inLemma = Word(dep.inLemma, simplifyPos(dep.inPos))
          if (id2dependency.contains(dep.outID)) {
            val outLemma = Word(id2dependency(dep.outID).inLemma, simplifyPos(id2dependency(dep.outID).inPos))

            if (!lemmas2features.contains(inLemma)) {
              lemmas2features(inLemma) = new ListBuffer[Feature]()
            }
            if (!lemmas2features.contains(outLemma)) {
              lemmas2features(outLemma) = new ListBuffer[Feature]()
            }

            if (dep.inID != dep.outID && dep.dtype.toLowerCase != "root") {
              if (svoOnly) {
                if (dep.dtype.contains("subj") || dep.dtype.contains("obj")) {
                  lemmas2features(inLemma).append(Feature(outLemma, simplifyDtype(dep.dtype)))
                  lemmas2features(outLemma).append(Feature(inLemma, simplifyDtype(dep.dtype)))
                }
              } else {
                lemmas2features(inLemma).append(Feature(outLemma, simplifyDtype(dep.dtype)))
                lemmas2features(outLemma).append(Feature(inLemma, simplifyDtype(dep.dtype)))
              }
            }
          } else {
            if (verbose) println(s"Warning: dep.outID not present:\t@:${dep.outID}--${dep.dtype}--${dep.inLemma}#${dep.inPos}:${dep.inID}")
            depErrCount.add(1)
          }
        }

        // keep only tokens of interest if filter is enabled
        val lemmas2featuresPos = if (verbsOnly) {
          lemmas2features.filter {
            case (word, features) => verbPos.contains(word.pos) && features.length > 0
          }
        } else {
          lemmas2features
        }

        // output the features
        for ((lemma, features) <- lemmas2featuresPos)
          yield (lemma, features)
      }
      .cache()

    if (saveIntermediate){
      val unaggregatedFeaturesPath = outputFeaturesDir + "/unaggregated"
      unaggregatedFeatures
        .map { case (lemma, features) => s"${lemma.lemma}${Const.POS_SEP}${lemma.pos}\t${features.mkString("\t")}" }
        .saveAsTextFile(unaggregatedFeaturesPath)
    }

    val aggregatedFeaturesPath = outputFeaturesDir + "/aggregated"

    // Feature counts
    val featureCountsPath = aggregatedFeaturesPath + "/F"
    val featureCounts = unaggregatedFeatures
      .flatMap{ case (lemma, features) => for (f <- features) yield (f, 1) }
      .reduceByKey{ _ + _ }
      .cache()
    if (saveIntermediate) {
      featureCounts
        .sortBy(_._2, ascending = false)
        .map { case (feature, freq) => s"$feature\t$freq" }
        .saveAsTextFile(featureCountsPath)
    }

    // Word counts
    val wordCountsPath = aggregatedFeaturesPath + "/W"
    val wordCounts = unaggregatedFeatures
      .map{ case (lemma, features) =>  (s"${lemma.lemma}${Const.POS_SEP}${lemma.pos}", 1) }
      .reduceByKey{ _ + _ }
      .cache()
    if (saveIntermediate) {
      wordCounts
        .sortBy(_._2, ascending = false)
        .map { case (word, freq) => s"$word\t$freq" }
        .saveAsTextFile(wordCountsPath)
    }

    // WF i.e. the SVO counts
    val wordFeatureCountsPath = aggregatedFeaturesPath + "/WF"
    val wordFeatureCounts = unaggregatedFeatures
      .flatMap{ case (lemma, features) =>
        var subjs = ListBuffer[Feature]()
        var objs = ListBuffer[Feature]()

        for (f <- features){
          if (f.dtype.contains("subj")) subjs.append(f)
          else if (f.dtype.contains("obj")) objs.append(f)
        }

        for (s <- subjs; o <- objs) yield ((lemma, s, o), 1)
      }
      .reduceByKey{ _ + _ }
      .cache()

    if (saveIntermediate) {
      wordFeatureCounts
        .sortBy(_._2, ascending = false)
        .map { case (wso, freq) => s"${wso._1.lemma}${Const.POS_SEP}${wso._1.pos}\t${wso._2}\t${wso._3}\t$freq" }
        .saveAsTextFile(wordFeatureCountsPath)
     }

//    // Pruned WF (SVO counts)
//    val wordFeatureCountsPerWordPath = aggregatedFeaturesPath + "/WF-W"
//    val wordFeatuesPerWord = wordFeatureCounts
//      .map { case (wso, freq) => ((wso._1), (wso._2, wso._3, freq))}
//      .groupByKey()
//      .cache()
//
//    if (saveIntermediate) {
//      wordFeatuesPerWord
//        .map { case (w, args) =>  s"${w._1}${Const.POS_SEP}${w._2}\t${args.mkString(", ")}" }
//        .saveAsTextFile(wordFeatureCountsPerWordPath)
//    }

  }
}

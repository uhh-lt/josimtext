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

  object NormType extends Enumeration {
    val PMI, LMI, LMIU = Value
  }

  case class Word(lemma: String, pos: String) {
    override def toString: String =
      s"$lemma${Const.POS_SEP}$pos"
  }

  case class Feature(word: Word, dtype: String) {
    override def toString: String =
      s"$dtype${Const.POS_SEP}$word"
  }

  case class WordFeature(word: Word, feature: Feature) {
    override def toString: String =
      s"$word${Const.POS_SEP}$feature"
  }

  val verbPos = Set("VB", "VBZ", "VBD", "VBN", "VBP", "MD")
  val verbose = false
  val conllRecordDelimiter = ">>>>>\t"
  val svoOnly = true // subject-verb-object features only
  val saveIntermediate = false
  val minFreq = 5
  val normType = NormType.LMIU
  val minSim = 0.20

  case class Dependency(inID: Int, inToken: String, inLemma: String, inPos: String, outID: Int, dtype: String) {
    def this(fields: Array[String]) = {
      this(fields(0).toInt, fields(1), fields(2), fields(4), fields(6).toInt, fields(7))
    }
  }

  case class ScoreExp(score: Double, explanation: String) {
    def +(that: ScoreExp): ScoreExp = {
      new ScoreExp(this.score + that.score,
        this.explanation + ", " + that.explanation)
    }

    def add(that: ScoreExp): ScoreExp = this.+(that)
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

  def simplifyDtype(fullDtype: String) = {
    if (fullDtype.contains("subj")) "subj"
    else if (fullDtype.contains("obj")) "obj"
    else fullDtype
  }

  def simplifyPos(fullPos: String) = {
    fullPos.substring(0, Math.min(2, fullPos.length))
    //CoarsifyPosTags.full2coarse(fullPos)
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

    if (saveIntermediate) {
      val unaggregatedFeaturesPath = outputFeaturesDir + "/unaggregated"
      unaggregatedFeatures
        .map { case (lemma, features) => s"$lemma\t${features.mkString("\t")}" }
        .saveAsTextFile(unaggregatedFeaturesPath)
    }

    // Feature counts
    val featureCountsPath = outputFeaturesDir + "/F"
    val featureCounts: RDD[(Feature, Long)] = unaggregatedFeatures
      .flatMap { case (lemma, features) => for (f <- features) yield (f, 1.toLong) }
      .reduceByKey {
        _ + _
      }
      .filter { case (feature, freq) => freq >= minFreq }
      .cache()

    featureCounts
      .sortBy(_._2, ascending = false)
      .map { case (feature, freq) => s"$feature\t$freq" }
      .saveAsTextFile(featureCountsPath)

    // Word counts
    val wordCountsPath = outputFeaturesDir + "/W"
    val wordCounts: RDD[(Word, Long)] = unaggregatedFeatures
      .map { case (lemma, features) => (lemma, 1.toLong) }
      .reduceByKey {
        _ + _
      }
      .filter { case (feature, freq) => freq >= minFreq }
      .cache()

    wordCounts
      .sortBy(_._2, ascending = false)
      .map { case (word, freq) => s"$word\t$freq" }
      .saveAsTextFile(wordCountsPath)


    // WF i.e. the SVO counts
    val wordFeatureCounts = unaggregatedFeatures
      .flatMap { case (lemma, features) =>
        var subjs = ListBuffer[Feature]()
        var objs = ListBuffer[Feature]()

        for (f <- features) {
          if (f.dtype.contains("subj")) subjs.append(f)
          else if (f.dtype.contains("obj")) objs.append(f)
        }

        for (s <- subjs; o <- objs) yield ((lemma, s, o), 1.toLong)
      }
      .reduceByKey {
        _ + _
      }
      .filter { case (wso, freq) => freq >= minFreq }
      .map { case (wso, freq) => (wso._1, wso._2, wso._3, freq.toDouble) }
      .cache()
    val wordFeatureCountsPath = outputFeaturesDir + "/wf"
    saveWF(wordFeatureCountsPath, wordFeatureCounts)

    // Pruned WF (SVO counts)
    val svoSum = wordFeatureCounts.map(_._4).sum()
    val wordFeaturesNorm: RDD[(Word, Feature, Feature, Double)] = wordFeatureCounts
      .map { case (w, s, o, freq) => (w, (s, o, freq)) }
      .join(wordCounts)
      .map { case (w, ((s, o, wsoFreq), wFreq)) => (s, (w, o, wFreq, wsoFreq)) }
      .join(featureCounts)
      .map { case (s, ((w, o, wFreq, wsoFreq), sFreq)) => (o, (w, s, wFreq, wsoFreq, sFreq)) }
      .join(featureCounts)
      .map { case (o, ((w, s, wFreq, wsoFreq, sFreq), oFreq)) => (w, s, o, wFreq, sFreq, oFreq, wsoFreq) }
      .map { case (w, s, o, wFreq, sFreq, oFreq, wsoFreq) =>
        (w, s, o, norm(svoSum, wFreq, sFreq, oFreq, wsoFreq))
      }
      .cache()
    val wordFeaturesNormPath = outputFeaturesDir + "/wf-norm"
    saveWF(wordFeaturesNormPath, wordFeaturesNorm)

    // Grouped WF by word
    val wordFeatureCountsPerWordPath = outputFeaturesDir + "/wf-w"
    val wordFeatuesPerWord = aggregateWordFeatures(
      wordFeatureCounts,
      wordFeatureCountsPerWordPath,
      false)

    val wordFeatureCountsNormPerWordPath = outputFeaturesDir + "/wf-norm-w"
    val wordFeatuesNormPerWord = aggregateWordFeatures(
      wordFeaturesNorm,
      wordFeatureCountsNormPerWordPath,
      normType == NormType.LMIU)

    val simsDebug = wordFeatuesNormPerWord
      .flatMap { case (w, features) =>
        for (f <- features) yield ((f._1, f._2), (w, f._3))
      }
      .groupByKey()
      .flatMap { case ((s, o), words) =>
        for (w_i <- words; w_j <- words) yield ((w_i._1, w_j._1),
          ScoreExp(w_i._2 * w_j._2, s"${s.word.lemma}_${o.word.lemma}"))
      }
      .reduceByKey(_ + _)
      .sortBy(r => (s"${r._1._1.lemma}#${r._1._1.pos}", r._2.score), ascending = false)
      .map { case ((w_i, w_j), scoreexp) =>
        "%s#%s\t%s#%s\t%.8f\t%s".format(w_i.lemma, w_i.pos, w_j.lemma, w_j.pos, scoreexp.score, scoreexp.explanation)
      }
      .saveAsTextFile(outputFeaturesDir + "/sims-explained")

    // Similarity of words
    val sims = wordFeatuesNormPerWord
      .flatMap { case (w, features) =>
        for (f <- features) yield ((f._1, f._2), (w, f._3))
      }
      .groupByKey()
      .flatMap { case ((s, o), words) =>
        for (w_i <- words; w_j <- words) yield ((w_i._1, w_j._1), w_i._2 * w_j._2)
      }
      .reduceByKey(_ + _)
      .filter { case ((w_i, w_j), score) => w_i != w_j }
      .filter { case ((w_i, w_j), score) => score >= minSim }
      .cache()

    val simsPath = outputFeaturesDir + "/sims"
    sims
      .sortBy(r => (s"${r._1._1.lemma}#${r._1._1.pos}", r._2), ascending = false)
      .map { case ((w_i, w_j), score) => s"${w_i.lemma}#${w_i.pos}\t${w_j.lemma}#${w_j.pos}\t$score" }
      .saveAsTextFile(simsPath)
  }

  private def saveWF(wordFeatureCountsPath: String, wordFeatureCounts: RDD[(Word, Feature, Feature, Double)]) = {
    wordFeatureCounts
      .sortBy(_._4, ascending = false)
      .map { case (w, s, o, score) => s"$w\t${s.word}_${o.word}\t$score" }
      .saveAsTextFile(wordFeatureCountsPath)
  }

  private def aggregateWordFeatures(wordFeatureCounts: RDD[(Word, Feature, Feature, Double)], wordFeatureCountsPerWordPath: String, unitNorm: Boolean) = {
    val wordFeatuesPerWord = wordFeatureCounts
      .map { case (w, s, o, freq) => (w, (s, o, freq)) }
      .groupByKey()

    val result = if (unitNorm) {
      wordFeatuesPerWord
        .flatMap { case (w, features) =>
          var norm = Math.sqrt(features
            .map {
              _._3
            }
            .map { case s => s * s }
            .sum
          )
          if (norm == 0) norm = 1.0
          for (f <- features) yield (w, (f._1, f._2, f._3 / norm))
        }
        .groupByKey()
    } else {
      wordFeatuesPerWord
    }

    result.cache()

    result
      .map { case (w, args) => s"${w.lemma}#${w.pos}\t${
        args
          .toList.sortBy(-_._3)
          .map { case (s, o, score) => "%s#%s_%s#%s:%.8f".format(
            s.word.lemma, s.word.pos, o.word.lemma, o.word.pos, score)
          }
          .mkString(", ")
      }"
      }
      .saveAsTextFile(wordFeatureCountsPerWordPath)

    result
  }

  private def norm(svoSum: Double, wFreq: Long, sFreq: Long, oFreq: Long, wsoFreq: Double) = {
    val pmi = (svoSum * svoSum * wsoFreq) / (wFreq.toDouble * sFreq.toDouble * oFreq.toDouble)
    normType match {
      case NormType.PMI => pmi
      case NormType.LMI => wsoFreq * pmi
      case NormType.LMIU => wsoFreq * pmi
    }
  }
}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer
import scala.util.Try

object Conll2Features {
  /* CoNLL format: for each dependency output a field with ten columns ending with the bio named entity: http://universaldependencies.org/docs/format.html
     IN_ID TOKEN LEMMA POS_COARSE POS_FULL MORPH ID_OUT TYPE _ NE_BIO
     5 books book NOUN NNS Number=Plur 2 dobj 4:dobj SpaceAfter=No */

  val verbPos = Set("VB", "VBZ", "VB")
  val verbose = false

  case class Dependency(inID:Int, inToken:String, inLemma:String, inPos:String, outID:Int, dtype:String) {
    def this(fields: Array[String]) = {
      this(fields(0).toInt, fields(1), fields(2), fields(4), fields(6).toInt, fields(7))
    }
  }

  def main(args: Array[String]) {
      if (args.size < 2) {
        println("Parameters: <input-dir> <output-dir>")
        println("<input-dir>\tDirectory with a parsed corpus in the CoNLL format.'")
        println("<output-dir>\tDirectory with an output word feature files")
        return
      }

      val inputPath = args(0)
      val outputPath = args(1)

      val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(conf)

      run(sc, inputPath, outputPath)
    }

    def run(sc: SparkContext, inputConllDir: String, outputFeaturesDir: String) = {
      println("Input dir.: " + inputConllDir)
      println("Output dir.: " + outputFeaturesDir)

      Util.delete(outputFeaturesDir) // a convinience for the local tests

      val conf = new Configuration
      conf.set("textinputformat.record.delimiter", ">>>>>\t")
      val dataset = sc.newAPIHadoopFile(inputConllDir, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
      val data = dataset
        .map{ record => record._2.toString }
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
          val lemmas2features = collection.mutable.Map[String, ListBuffer[String]]()
          var depCount = 0
          var errCount = 0
          for ((id, dep) <- id2dependency) {
            depCount += 1

            val inLemma = s"${dep.inLemma}#${dep.inPos}"
              if (id2dependency.contains(dep.outID)) {
                val outLemma = s"${id2dependency(dep.outID).inLemma}#${id2dependency(dep.outID).inPos}"

                if (!lemmas2features.contains(inLemma)) {
                  lemmas2features(inLemma) = new ListBuffer[String]()
                }
                if (!lemmas2features.contains(outLemma)) {
                  lemmas2features(outLemma) = new ListBuffer[String]()
                }

                if (dep.inID != dep.outID && dep.dtype != "ROOT") {
                  lemmas2features(inLemma).append(s"@--${dep.dtype}--${outLemma}")
                  lemmas2features(outLemma).append(s"${inLemma}--${dep.dtype}--@")
                }

              } else {
                if (verbose) println(s"Warning: dep.outID not present:\t@:${dep.outID}--${dep.dtype}--${dep.inLemma}#${dep.inPos}:${dep.inID}")
                errCount += 1
              }
          }

          // generate the features
//          for ((lemma, features) <- lemmas2features; feature <- features)
//              yield (lemma + "\t" + feature, 1)

          for ((lemma, features) <- lemmas2features)
            //val feature = features.mkString("+");
            yield (lemma + "\t" + features.mkString("\t"), 1)

        }
        //.map{ line => line.split("\t") }
        //.map{ case Array(inID, token, lemma, posCoarse, posFull, morph, outID, dtype, x, bio) =>
        //  (Try(inID.toLong).getOrElse(-1), Try(outID.toLong).getOrElse(-1), dtype, lemma, posFull)
        //  case _ => (-10, "?", "?", "?", "?", "?", -10, "?", "?", "?") }
        //.map{ case (word, freq) => (coarsifyPosTag(word), freq) }
        //.reduceByKey{ _ + _ }
        //.map{ case (word,freq) => (freq, word)}
        //.sortByKey(ascending = false)
        //.map{ case (freq, word) => s"$word\t$freq" }
        .map{ case (f, s) => s"${f}\t${s}" }
        .saveAsTextFile(outputFeaturesDir)

    }
  }

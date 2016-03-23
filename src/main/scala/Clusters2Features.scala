import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import scala.util.Try

object Clusters2Features {
  val _stopwords = Util.getStopwords()

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Makes a features file from the senses.")
      println("Usage: <senses> <word-counts> <output>")
      return
    }

    // Initialization
    val conf = new SparkConf().setAppName("JST: Clusters2Features")
    val sc = new SparkContext(conf)

    val sensesPath = args(0)
    val wordsPath = args(1)
    val outputPath = args(2)

    println("Senses: " + sensesPath)
    println("Words: " + wordsPath)
    println("Output: " + outputPath)
    Util.delete(outputPath)


    val clusters = sc
      .textFile(sensesPath)
      .map(line => line.split("\t"))
      .map{ case Array(target, sense_id, keyword, cluster) => (target, sense_id, keyword, cluster) case _ => ("?", "-1", "?", "") }
      .filter{ case (target, sense_id, keyword, cluster) => target != "?" }

    val clusterSimWords: RDD[((String, String), (Array[(String, Double)], Double))] = clusters  // (target, sense_id), [((word, sim), sim_sum)]
      .map{ case (target, sense_id, keyword, cluster) => (
        (target, sense_id),
        cluster.split(Const.LIST_SEP)
          .map(wordWithSim => Util.splitLastN(wordWithSim, Const.SCORE_SEP, 2))
          .map{ case Array(word, sim) =>  if (Try(sim.toDouble).isSuccess) (word.trim(), sim.toDouble) else (word.trim(), 0.0) case _ => ("?", 0.0) }
          .filter({ case (word, sim) => !_stopwords.contains(word) })  )}
      .map{ case ((word, sense), simWords) => ((word, sense), (simWords, simWords.map(_._2).sum)) }
      .cache()

    val clusterSimSums:RDD[((String, String), Double)] = clusterSimWords  // (target, sense_id), sim_sum
      .map({case ((word, sense), (simWords, simSum)) => ((word, sense), simSum)})
      .cache()

    val wordCounts:RDD[(String, Long)] = sc  // word, freq
      .textFile(wordsPath)
      .map(line => line.split("\t"))
      .map{case Array(word, freq) => (word, freq.toLong)}


    val clusterWords:RDD[(String, (String, Double, String, Double))] = clusterSimWords  // word, (target, sim, sense_id, sim_sum)
      .flatMap({case ((word, sense), (simWordsWithSim, simSum)) => for((simWord, sim) <- simWordsWithSim) yield (simWord, (word, sim, sense, simSum))})

    val wordSenseCounts: RDD[((String, String), Double)] = clusterWords  // (target, sense_id), count
      .join(wordCounts)
      .map({case (simWord, ((word, sim, sense, simSum), wc)) => ((word, sense), wc*sim)})
      .reduceByKey(_+_)
      .join(clusterSimSums)
      .mapValues({case (wcSum, simSum) => wcSum/simSum})

    val result = clusters
        .map{case (target, sense_id, keyword, cluster) => ((target, sense_id), cluster)}
        .join(wordSenseCounts)
        .map{case ((target, sense_id), (cluster, score)) => f"$target\t$sense_id\tword\t${score.toInt}\t$cluster"}  // ((ruby,2),(linux:0.011  Python:0.009  java:0.008  perl:0.008  python:0.008,3007.7272727272725))
        .saveAsTextFile(outputPath)


  }


}

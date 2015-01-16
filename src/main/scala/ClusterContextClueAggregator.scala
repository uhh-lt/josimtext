import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object ClusterContextClueAggregator {
    def main(args: Array[String]) {
        if (args.size < 3) {
            println("Usage: ClusterContextClueAggregator cluster-file word-counts word-feature-counts output [min. wfc] [wordlist]")
            return
        }

        val t_wfc = if (args.length > 4) args(4).toDouble else 0.0

        val words:Set[String] = if (args.length > 5) args(5).split(",").toSet else null

        val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
        val sc = new SparkContext(conf)

        val clusterFile = sc.textFile(args(0))
        val wordCountFile = sc.textFile(args(1))
        val featureFile = sc.textFile(args(2))
        val outputFile = args(3)

        val clusterSimWords:RDD[((String, String), Array[String])] = clusterFile
            .map(line => line.split("\t"))
            .map(cols => ((cols(0), cols(1) + "\t" + cols(2)), cols(3).split("  ")))
            .filter({case ((word, sense), simWords) => words == null || words.contains(word)})

        val wordCounts:RDD[(String, Long)] = wordCountFile
            .map(line => line.split("\t"))
            .map(cols => (cols(0), cols(1).toLong))

        val clusterWords:RDD[(String, (String, String, Int))] = clusterSimWords
            .flatMap({case ((word, sense), simWords) => for(simWord <- simWords) yield (simWord, (word, sense, simWords.size))})

        val wordFeatures = featureFile
            .map(line => line.split("\t"))
            .map(cols => (cols(0), (cols(1), cols(2).toLong))) // (word, (feature, wfc))
            .filter({case (word, (feature, wfc)) => wfc >= t_wfc})

        val wordSenseCounts = clusterWords
            .join(wordCounts)
            .map({case (simWord, ((word, sense, numSimWords), wc)) => ((word, sense), wc)})
            .reduceByKey(_+_)

        clusterWords
            .join(wordFeatures)
            .map({case (simWord, ((word, sense, numSimWords), (feature, wfc))) => ((word, sense, feature), (wfc, numSimWords))})
            // Pretend cluster words are replaced with the same placeholder word and combine their word-feature counts:
            .reduceByKey({case ((wfc1, numSimWords), (wfc2, _)) => (wfc1+wfc2, numSimWords)})
            .map({case ((word, sense, feature), (wfc, numSimWords)) => ((word, sense), (feature, wfc))})
            .groupByKey()
            .join(wordSenseCounts)
            .map({case ((word, sense), (senseFeatureCounts, senseCount)) => ((word, sense), (senseCount, senseFeatureCounts))})
            .join(clusterSimWords)
            .sortByKey()
            .map({case ((word, sense), ((senseCount, senseFeatureCounts), simWords)) => word + "\t" + sense + "\t" + senseCount + "\t" + simWords.mkString("  ") + "\t" + senseFeatureCounts.map(tuple => tuple._1 + ":" + tuple._2).mkString("  ")})
            .saveAsTextFile(outputFile)
    }
}

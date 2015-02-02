import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object ClusterContextClueAggregator {
    def main(args: Array[String]) {
        if (args.size < 5) {
            println("Usage: ClusterContextClueAggregator cluster-file word-counts feature-counts word-feature-counts output [min. wfc] [wordlist]")
            return
        }

        val t_wfc = if (args.length > 5) args(5).toDouble else 0.0

        val words:Set[String] = if (args.length > 6) args(6).split(",").toSet else null

        val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
        val sc = new SparkContext(conf)

        val clusterFile = sc.textFile(args(0))
        val wordCountFile = sc.textFile(args(1))
        val featureCountFile = sc.textFile(args(2))
        val wordFeatureCountFile = sc.textFile(args(3))
        val outputFile = args(4)

        val clusterSimWords:RDD[((String, String), Array[(String, Double)])] = clusterFile
            .map(line => line.split("\t"))
            .map(cols => ((cols(0),
                           cols(1) + "\t" + cols(2)),
                           cols(3).split("  ")
                                  .map(wordWithSim => Util.splitLastN(wordWithSim, ':', 2))
                                  .map({case Array(word, sim) => (word, sim.toDouble)})))
            .filter({case ((word, sense), simWords) => words == null || words.contains(word)})

        val wordCounts:RDD[(String, Long)] = wordCountFile
            .map(line => line.split("\t"))
            .map(cols => (cols(0), cols(1).toLong))

        val featureCounts:RDD[(String, Long)] = featureCountFile
            .map(line => line.split("\t"))
            .map(cols => (cols(0), cols(1).toLong))

        val clusterWords:RDD[(String, (String, String, Int))] = clusterSimWords
            .flatMap({case ((word, sense), simWordsWithSim) => for((simWord, sim) <- simWordsWithSim) yield (simWord, (word, sense, simWordsWithSim.size))})

        val wordFeatures = wordFeatureCountFile
            .map(line => line.split("\t"))
            .map(cols => (cols(1), (cols(0), cols(2).toLong))) // (feature, (word, wfc))
            .join(featureCounts)
            .map({case (feature, ((word, wfc), fc)) => (word, (feature, wfc, fc))})
            .filter({case (word, (feature, wfc, fc)) => wfc >= t_wfc})

        val wordSenseCounts = clusterWords
            .join(wordCounts)
            .map({case (simWord, ((word, sense, numSimWords), wc)) => ((word, sense), wc)})
            .reduceByKey(_+_)

        clusterWords
            .join(wordFeatures)
            .map({case (simWord, ((word, sense, numSimWords), (feature, wfc, fc))) => ((word, sense, feature), (wfc, fc, numSimWords))})
            // Pretend cluster words are replaced with the same placeholder word and combine their word-feature counts:
            .reduceByKey({case ((wfc1, fc, numSimWords), (wfc2, _, _)) => (wfc1+wfc2, fc, numSimWords)})
            .map({case ((word, sense, feature), (wfc, fc, numSimWords)) => ((word, sense), (feature, wfc, fc))})
            .groupByKey()
            .join(wordSenseCounts)
            .map({case ((word, sense), (senseFeatureCounts, senseCount)) => ((word, sense), (senseCount, senseFeatureCounts))})
            .join(clusterSimWords)
            .sortByKey()
            .map({case ((word, sense), ((senseCount, senseFeatureCounts), simWordsWithSim)) => word + "\t" + sense + "\t" + senseCount + "\t" + simWordsWithSim.map({case (simWord, sim) => simWord + ":" + sim}).mkString("  ") + "\t" + senseFeatureCounts.map(tuple => tuple._1 + ":" + tuple._2 + ":" + tuple._3).mkString("  ")})
            .saveAsTextFile(outputFile)
    }
}

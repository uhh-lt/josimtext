import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object ClusterContextClueAggregator {
    def main(args: Array[String]) {
        if (args.size < 3) {
            println("Usage: ClusterContextClueAggregator cluster-file feature-file output [min. probability] [min. coverage] [wordlist]")
            return
        }

        val param_s = if (args.length > 3) args(3).toDouble else 0.0
        val param_p = if (args.length > 4) args(4).toDouble else 0.0

        val words:Set[String] = if (args.length > 5) args(5).split(",").toSet else null

        val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
        val sc = new SparkContext(conf)

        val clusterFile = sc.textFile(args(0))
        val featureFile = sc.textFile(args(1))
        val outputFile = args(2)

        val clusterSimWords:RDD[((String, String), Array[String])] = clusterFile
            .map(line => line.split("\t"))
            .map(cols => ((cols(0), cols(1) + "\t" + cols(2)), cols(3).split("  ")))
            .filter({case ((word, sense), simWords) => words == null || words.contains(word)})

        val clusterWords:RDD[(String, (String, String, Int))] = clusterSimWords
            .flatMap({case ((word, sense), simWords) => for(simWord <- simWords) yield (simWord, (word, sense, simWords.size))})

        val wordFeatures = featureFile
            .map(line => line.split("\t"))
            .map(cols => (cols(0), (cols(1), cols(2).toLong, cols(3).toLong, cols(4).toLong, cols(5).toLong))) // (feature, wc, fc, wfc, n)
            .filter({case (word, (feature, wc, fc, wfc, n)) => wfc.toDouble / fc.toDouble >= param_s && wfc.toDouble / wc.toDouble >= param_p})

        val featuresPerWord = wordFeatures
            .mapValues(feature => 1)
            .reduceByKey((sum1, sum2) => sum1 + sum2)

        clusterWords
            .join(wordFeatures)
            .map({case (simWord, ((word, sense, numSimWords), (feature, wc, fc, wfc, n))) => ((word, sense, feature), (wc, fc, wfc, n, wfc / fc.toDouble, wfc / wc.toDouble, numSimWords))})
            // Pretend cluster words are replaced with the same placeholder word and combine their counts:
            .reduceByKey({case ((wc1, fc, wfc1, n, prob1, cov1, numSimWords), (wc2, _, wfc2, _, prob2, cov2, _)) => (wc1+wc2, fc, wfc1+wfc2, n, prob1+prob2, cov1+cov2, numSimWords)})
            .map({case ((word, sense, feature), (wc, fc, wfc, n, prob, cov, numSimWords)) => ((word, sense, wc), (feature, WordSimUtil.lmi(n, wc, fc, wfc), prob / numSimWords, cov / numSimWords, wc, fc, wfc, n))})
            .groupByKey()
            .map({case ((word, sense, senseCount), featureScores) => ((word, sense), (senseCount, featureScores.toArray.sortWith({case ((_, lmi1, _, _, _, _, _, _), (_, lmi2, _, _, _, _, _, _)) => lmi1 > lmi2})))})
            .join(clusterSimWords)
            .sortByKey()
            .map({case ((word, sense), ((senseCount, featureScores), simWords)) => word + "\t" + sense + "\t" + senseCount + "\t" + simWords.mkString("  ") + "\t" + featureScores.map({case (feature, lmi, avgProb, avgCov, wc, fc, wfc, n) => feature + ":" + lmi + ":" + avgProb + ":" + avgCov + ":" + wc + ":" + fc + ":" + wfc + ":" + n}).mkString("  ")})
            .saveAsTextFile(outputFile)
    }
}

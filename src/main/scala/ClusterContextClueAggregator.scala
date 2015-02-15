import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object ClusterContextClueAggregator {
    def main(args: Array[String]) {
        if (args.size < 6) {
            println("Usage: ClusterContextClueAggregator cluster-file word-counts feature-counts word-feature-counts output_suffix num_sim_words [min. wfc] [wordlist]")
            return
        }

        val t_wfc = if (args.length > 6) args(6).toDouble else 0.0

        val words:Set[String] = if (args.length > 7) args(7).split(",").toSet else null

        val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
        val sc = new SparkContext(conf)

        val clusterFile = sc.textFile(args(0))
        val wordCountFile = sc.textFile(args(1))
        val featureCountFile = sc.textFile(args(2))
        val wordFeatureCountFile = sc.textFile(args(3))
        val outputSuffix = args(4)
        val numSimWords = args(5).toInt
        val outputFile = args(0) + "__" + outputSuffix

        val clusterSimWords:RDD[((String, String), (Array[(String, Double)], Double))] = clusterFile
            .map(line => line.split("\t"))
            .map(cols => ((cols(0),
                           cols(1) + "\t" + cols(2)),
                           cols(3).split("  ")
                                  .take(numSimWords)
                                  .map(wordWithSim => Util.splitLastN(wordWithSim, ':', 2))
                                  .map({case Array(word, sim) => (word, sim.toDouble)})))
            .filter({case ((word, sense), simWords) => words == null || words.contains(word)})
            .map({case ((word, sense), simWords) => ((word, sense), (simWords, simWords.map(_._2).sum))})
            .cache()

        /*val clusterSimSums:RDD[((String, String), Double)] = clusterSimWords
            .map({case ((word, sense), (simWords, simSum)) => ((word, sense), simSum)})
            .cache()*/

        val wordCounts:RDD[(String, Long)] = wordCountFile
            .map(line => line.split("\t"))
            .map(cols => (cols(0), cols(1).toLong))

        val featureCounts:RDD[(String, Long)] = featureCountFile
            .map(line => line.split("\t"))
            .map(cols => (cols(0), cols(1).toLong))

        val clusterWords:RDD[(String, (String, Double, String, Double))] = clusterSimWords
            .flatMap({case ((word, sense), (simWordsWithSim, simSum)) => for((simWord, sim) <- simWordsWithSim) yield (simWord, (word, sim, sense, simSum))})

        val wordFeatures = wordFeatureCountFile
            .map(line => line.split("\t"))
            .map(cols => (cols(1), (cols(0), cols(2).toLong))) // (feature, (word, wfc))
            .filter({case (feature, (word, wfc)) => wfc >= t_wfc})
            .join(featureCounts)
            .map({case (feature, ((word, wfc), fc)) => (word, (feature, wfc, fc))})
            .join(wordCounts)
            .map({case (word, ((feature, wfc, fc), wc)) => (word, (feature, wc, fc, wfc))})

        val wordSenseCounts = clusterWords
            .join(wordCounts)
            .map({case (simWord, ((word, sim, sense, simSum), wc)) => ((word, sense), wc)})
            .reduceByKey(_+_)
            //.join(clusterSimSums)
            //.mapValues({case (wcSum, simSum) => wcSum/simSum})

        clusterWords
            .join(wordFeatures)
            .map({case (simWord, ((word, sim, sense, simSum), (feature, wc, fc, wfc))) => ((word, sense, feature), wfc)})
            // Pretend cluster words are replaced with the same placeholder word and combine their word-feature counts:
            .reduceByKey(_+_)
            //.map({case ((word, sense, feature), pSum) => ((word, sense), (feature, pSum))})
            //.join(clusterSimSums)
            .map({case ((word, sense, feature), wfcSum) => ((word, sense), (feature, wfcSum))})
            .join(wordSenseCounts)
            .map({case ((word, sense), ((feature, wfcSum), senseCount)) => ((word, sense), (feature, wfcSum / senseCount.toDouble))})
            .groupByKey()
            .join(wordSenseCounts)
            .map({case ((word, sense), (senseFeatureProbs, senseCount)) => ((word, sense), (senseCount, senseFeatureProbs.toList.sortBy(_._2).reverse))})
            .join(clusterSimWords)
            .sortByKey()
            .map({case ((word, sense), ((senseCount, senseFeatureProbs), (simWordsWithSim, simSum))) =>
                word + "\t" +
                sense + "\t" +
                senseCount + "\t" +
                simWordsWithSim.map({case (simWord, sim) => simWord + ":" + sim}).mkString("  ") + "\t" +
                    senseFeatureProbs.map(tuple => tuple._1 + ":" + tuple._2).mkString("  ")})
            .saveAsTextFile(outputFile)
    }
}

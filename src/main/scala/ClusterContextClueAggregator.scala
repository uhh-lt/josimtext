import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object ClusterContextClueAggregator {
    val stopwords = Util.getStopwords()

    def keepFeature(feature:String, dependencyFeature:Boolean) = {
        if (dependencyFeature){
            val (depType, srcWord, dstWord) = Util.parseDep(feature)
            if (Const.Resources.STOP_DEPENDENCIES.contains(depType) || stopwords.contains(srcWord) || stopwords.contains(dstWord)) {
                false
            } else {
                true
            }
        } else{
            true
        }
    }

    def main(args: Array[String]) {
        if (args.size < 6) {
            println("Usage: ClusterContextClueAggregator cluster-file word-counts feature-counts word-feature-counts output_suffix num_sim_words dep-features [min. wfc] [wordlist]")
            return
        }

        // Initialization
        val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
        val sc = new SparkContext(conf)

        val sensesPath = args(0)
        val wordsPath = args(1)
        val featuresPath = args(2)
        val wordFeaturesPath = args(3)
        val outputPath = args(4)
        val numSimWords = args(5).toInt
        val depFeatures = args(6).toBoolean
        val minWordFeatureCount = if (args.length > 7) args(7).toDouble else 0.0
        val targetWords:Set[String] = if (args.length > 8) args(8).split(",").toSet else null

        Util.delete(outputPath)

        println("Senses: " + sensesPath)
        println("Words: " + wordsPath)
        println("Word features: " + wordFeaturesPath)
        println("Features: " + featuresPath)
        println("Output: " + outputPath)
        println("Number of similar words from cluster: " + numSimWords)
        println("Dependency features: " + depFeatures)
        println("Minimum word feature count: " + minWordFeatureCount)
        println("Target words:" + targetWords)
        // Action

        val clusterSimWords = sc
            .textFile(sensesPath)
            .map(line => line.split("\t"))
            .map({case Array(target, sense_id, keyword, cluster) => (target, sense_id, keyword, cluster)})
            .map({case (target, sense_id, keyword, cluster) => (
                (target, sense_id + "\t" + keyword),
                cluster.split(Const.LIST_SEP)
                  .take(numSimWords)
                  .map(wordWithSim => Util.splitLastN(wordWithSim, Const.SCORE_SEP, 2))
                  .map({case Array(word, sim) => (word, sim.toDouble)})
                  .filter({ case (word, sim) => !stopwords.contains(word) })   )})
            .filter({case ((word, sense), simWords) => targetWords == null || targetWords.contains(word)})
            .map({case ((word, sense), simWords) => ((word, sense), (simWords, simWords.map(_._2).sum))})
            .cache()

        val clusterSimSums:RDD[((String, String), Double)] = clusterSimWords
            .map({case ((word, sense), (simWords, simSum)) => ((word, sense), simSum)})
            .cache()

        val wordCounts:RDD[(String, Long)] = sc
            .textFile(wordsPath)
            .map(line => line.split("\t"))
            .map({case Array(word, freq) => (word, freq.toLong)})

        val featureCounts:RDD[(String, Long)] = sc
            .textFile(featuresPath)
            .map(line => line.split("\t"))
            .map({case Array(word, freq) => (word, freq.toLong)})

        val clusterWords:RDD[(String, (String, Double, String, Double))] = clusterSimWords
            .flatMap({case ((word, sense), (simWordsWithSim, simSum)) => for((simWord, sim) <- simWordsWithSim) yield (simWord, (word, sim, sense, simSum))})

        val wordFeatures = sc
            .textFile(wordFeaturesPath)
            .map(line => line.split("\t"))
            .map(cols => (cols(1), (cols(0), cols(2).toLong))) // (feature, (word, wfc))
            .filter({case (feature, (word, wfc)) => wfc >= minWordFeatureCount})
            .join(featureCounts)
            .map({case (feature, ((word, wfc), fc)) => (word, (feature, wfc, fc))})
            .join(wordCounts)
            .map({case (word, ((feature, wfc, fc), wc)) => (word, (feature, wc, fc, wfc))})

        val wordSenseCounts = clusterWords
            .join(wordCounts)
            .map({case (simWord, ((word, sim, sense, simSum), wc)) => ((word, sense), wc*sim)})
            .reduceByKey(_+_)
            .join(clusterSimSums)
            .mapValues({case (wcSum, simSum) => wcSum/simSum})

      // Pretend cluster words are replaced with the same placeholder word and combine their word-feature counts
      clusterWords
            .join(wordFeatures)
            .map({case (simWord, ((word, sim, sense, simSum), (feature, wc, fc, wfc))) => ((word, sense, feature), sim*(wfc/wc.toDouble))})
            .reduceByKey(_+_)
            .map({case ((word, sense, feature), pSum) => ((word, sense), (feature, pSum))})
            .join(clusterSimSums)
            .map({case ((word, sense), ((feature, pSum), simSum)) => ((word, sense), (feature, pSum/simSum))})
            .groupByKey()
            .join(wordSenseCounts)
            .map({case ((word, sense), (senseFeatureProbs, senseCount)) => ((word, sense), (senseCount, senseFeatureProbs.toList.sortBy(_._2).reverse))})
            .join(clusterSimWords)
            .sortByKey()
            .map({case ((word, sense), ((senseCount, senseFeatureProbs), (simWordSim, simSum))) =>
                word + "\t" +
                sense + "\t" +
                senseCount + "\t" +
                simWordSim
                    .map({case (simWord, sim) => "%s%s%.6f".format(simWord, Const.SCORE_SEP, sim)})
                    .mkString(Const.LIST_SEP) + "\t" +
                senseFeatureProbs
                    .filter({case (feature, prob) => keepFeature(feature, depFeatures)})
                    .map({case (feature, prob) => "%s%s%.6f".format(feature, Const.SCORE_SEP, prob)})
                    .mkString(Const.LIST_SEP)})
            .saveAsTextFile(outputPath)
    }
}

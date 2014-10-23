import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordAndFeatureCount{
    def log2(n:Double): Double = {
        math.log(n) / math.log(2)
    }
    
    def ll(n:Long, wc:Long, fc:Long, bc:Long): Double = {
        val wcL = log2(wc)
        val fcL = log2(fc)
        val bcL = log2(bc)
        val epsilon = 0.000001
        val res = 2*(n*log2(n)
                            -wc*wcL
                            -fc*fcL
                            +bc*bcL
                            +(n-wc-fc+bc)*log2(n-wc-fc+bc+epsilon)
                            +(wc-bc)*log2(wc-bc+epsilon)
                            +(fc-bc)*log2(fc-bc+epsilon)
                            -(n-wc)*log2(n-wc+epsilon)
                            -(n-fc)*log2(n-fc+epsilon) )
        if ((n*bc)<(wc*fc)) -res.toDouble else res.toDouble
    }
    
    def lmi(n:Long, wc:Long, fc:Long, bc:Long): Double = {
        bc*math.log( (n*bc).toFloat/(wc*fc) )/math.log(2)
    }

    def main(args: Array[String]) {
        val param_t = 2
        val param_w = 1000
        val param_p = 1000
        val param_s = 0.0
        val param_l = 200

        val param_debug = false

        val dir = args(0)
        val conf = new SparkConf().setAppName("WordSim")
        val sc = new SparkContext(conf)
        val file = sc.textFile(dir)

        val wordFeaturesOccurrences = file
            .map(line => line.split("\t"))
            .map({case Array(word, feature, dataset, wordPos, featurePos) => (word, feature, dataset, wordPos, featurePos)
                  case _ => ("BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE")})
        wordFeaturesOccurrences.cache()

        val wordFeatureCounts = wordFeaturesOccurrences
            .map({case (word, feature, dataset, wordPos, featurePos) => ((word, feature, dataset, wordPos, featurePos), 1)})
            .reduceByKey((v1, v2) => v1 + v2) // count same occurences only once (make them unique)
            .map({case ((word, feature, dataset, wordPos, featurePos), numOccurrences) => ((word, feature), 1)})
            .reduceByKey((v1, v2) => v1 + v2)
            .map({case ((word, feature), count) => (word, (feature, count))})
        wordFeatureCounts.cache()

        val wordCounts = wordFeaturesOccurrences
            .map({case (word, feature, dataset, wordPos, featurePos) => ((word, dataset, wordPos), 1)})
            .reduceByKey((v1, v2) => v1 + v2)
            .map({case ((word, dataset, wordPos), numOccurrences) => (word, 1)})
            .reduceByKey((v1, v2) => v1 + v2)
        wordCounts.cache()

        val wordsPerFeature = wordFeatureCounts
            .map({case (word, (feature, wfc)) => (feature, word)})
            .groupByKey()
            .mapValues(v => v.toSet.size)
            .filter({case (feature, numWords) => numWords > 0 && numWords < param_w})

        val featureCounts = wordFeaturesOccurrences
            .map({case (word, feature, dataset, wordPos, featurePos) => ((feature, dataset, featurePos), 1)})
            .reduceByKey((v1, v2) => v1 + v2)
            .map({case ((feature, dataset, featurePos), numOccurrences) => (feature, 1)})
            .reduceByKey((v1, v2) => v1 + v2)
            .join(wordsPerFeature) // filter by using a join
            .map({case (feature, (fc, fwc)) => (feature, fc)}) // and remove unnecessary data from join
        featureCounts.cache()

        val wordFeatureCountsFiltered = wordFeatureCounts
            .filter({case (word, (feature, wfc)) => wfc >= param_t})
        wordFeatureCountsFiltered.cache()

        val n = wordFeatureCountsFiltered
            .map({case (word, (feature, wfc)) => (feature, (word, wfc))})
            //.join(wordsPerFeature) // filter by using a join
            //.map({case (feature, ((word, wfc), fwc)) => (word, (feature, wfc))})
            .aggregate(0L)(_ + _._2._2.toLong, _ + _) // we need Long because n might exceed the max. Int value

        val featuresPerWordWithScore = wordFeatureCountsFiltered
            .join(wordCounts)
            .map({case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc))})
            .join(featureCounts)
            .map({case (feature, ((word, wfc, wc), fc)) => (word, (feature, lmi(n, wc, fc, wfc)))})
            .filter({case (word, (feature, score)) => score >= param_s})
            .groupByKey()
            // (word, [(feature, score), (feature, score), ...])
            .mapValues(featureScores => featureScores.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2}).take(param_p)) // sort by value desc

        val wordsPerFeatureWithScore = featuresPerWordWithScore
            .flatMap({case (word, featureScores) => for(featureScore <- featureScores) yield (featureScore._1, (word, 1))})
            .groupByKey()
        wordsPerFeatureWithScore.cache()

        val wordSims = wordsPerFeatureWithScore
            .flatMap({case (feature, wordScores) => for((word1, score1) <- wordScores; (word2, score2) <- wordScores) yield ((word1, word2), score2)})
            .reduceByKey((score1, score2) => score1 + score2)
            .map({case ((word1, word2), score) => (word1, (word2, score))})
            .groupByKey()
            .mapValues(simWords => simWords.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2}).take(param_l))
            .flatMap({case (word, simWords) => for(simWord <- simWords) yield (word, simWord)})

        if (param_debug) {
            wordCounts
                .map({ case (word, count) => word + "\t" + count})
                .saveAsTextFile(dir + "__LMI_WordCount")
            featureCounts
                .map({ case (feature, count) => feature + "\t" + count})
                .saveAsTextFile(dir + "__LMI_FeatureCount")
            wordFeatureCounts
                .map({ case (word, (feature, count)) => word + "\t" + feature + "\t" + count})
                .saveAsTextFile(dir + "__LMI_WordFeatureCount")
            wordFeatureCountsFiltered
                .join(wordCounts)
                .map({ case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc))})
                .join(featureCounts)
                .map({ case (feature, ((word, wfc, wc), fc)) => word + "\t" + feature + "\t" + wc + "\t" + fc + "\t" + wfc + "\t" + n + "\t" + lmi(n, wc, fc, wfc)})
                .saveAsTextFile(dir + "__LMI_AllValuesPerWord")
            featuresPerWordWithScore
                .flatMap({ case (word, featureScores) => for (featureScore <- featureScores) yield (word, featureScore)})
                .map({ case (word, (feature, score)) => word + "\t" + feature + "\t" + score})
                .saveAsTextFile(dir + "__LMI_PruneGraph")
            wordsPerFeatureWithScore
                .map({ case (feature, wordList) => feature + "\t" + wordList.map(f => f._1).mkString("\t")})
                .saveAsTextFile(dir + "__LMI_AggrPerFeature")
        }

        wordSims.map({case (word1, (word2, sim)) => word1 + "\t" + word2 + "\t" + sim}).saveAsTextFile(dir + "__LMI_Sim")
    }
}

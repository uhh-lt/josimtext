import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/*
val res = wordFeatureCounts//.map(cols => (cols._2._1, (cols._1, cols._2._2)))
    .join(wordCounts)
    .map({case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc))})
    .join(featureCounts)
    .map({case (feature, ((word, wfc, wc), (fc, fwc))) => (word, (feature, ll(n, wc, fc, wfc)))})
    .filter({case (word, (feature, score)) => score >= param_s})
    .groupByKey()
    // (word, [(feature, score), (feature, score), ...])
    .mapValues(featureScores => featureScores.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2})
    .take(param_p)
    .map(featureScore => featureScore._1)) // sort by value desc
res.cache

// TODO: Implemenetiere map-reduce-ansatz ähnlich wie in jobimtext, dann können die features auch gewichtet werden
val res2 = res.cartesian(res)
    .map({case ((word1, features1), (word2, features2)) => (word1, (word2, features1.toSet.intersect(features2.toSet).size))})
    .filter({case (word1, (word2, sim)) => sim > 0})
*/

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
        val param_t = 2;
        val param_w = 1000;
        val param_p = 1000;
        val param_s = 0.0;
        val param_l = 200;

        val dir = args(0)
        val conf = new SparkConf().setAppName("WordSim")
        val sc = new SparkContext(conf)
        val file = sc.textFile(dir)
        val wordFeatureCounts = file
            .map(line => line.split("	"))
            .map(cols => (cols(0), (cols(1), cols(2).toInt)))
        
        wordFeatureCounts.cache()

        val wordCounts = wordFeatureCounts
            .map({case (word, (feature, wfc)) => (word, wfc)})
            .reduceByKey((wc1, wc2) => wc1 + wc2)
        wordCounts.cache()

        wordCounts
            .map({case (word, count) => word + "\t" + count})
            .saveAsTextFile(dir + "__LMI_WordCount")

        val wordsPerFeature = wordFeatureCounts
            .map({case (word, (feature, wfc)) => (feature, word)})
            .groupByKey()
            .mapValues(v => v.toSet.size)
            .filter({case (feature, numWords) => numWords > 0 && numWords < param_w})

        val featureCounts = wordFeatureCounts
            .map({case (word, (feature, wfc)) => (feature, wfc)})
            .reduceByKey((fc1, fc2) => fc1 + fc2)
            .join(wordsPerFeature) // filter by using a join
            .map({case (feature, (fc, fwc)) => (feature, fc)}) // and remove unnecessary data from join
        featureCounts.cache()

        featureCounts
            .map({case (feature, count) => feature + "\t" + count})
            .saveAsTextFile(dir + "__LMI_FeatureCount")

        val wordFeatureCountsFiltered = wordFeatureCounts
            .filter({case (word, (feature, wfc)) => wfc >= param_t})
        wordFeatureCountsFiltered.cache()

        // FIXME: n should not be based on any filtered counts! Use unfiltered word-feature-counts instead,
        // as feature and word counts are based on the unfiltered counts as well
        val n = wordFeatureCountsFiltered
            .map({case (word, (feature, wfc)) => (feature, (word, wfc))})
            .join(wordsPerFeature) // filter by using a join
            .map({case (feature, ((word, wfc), fwc)) => (word, (feature, wfc))})
            .aggregate(0L)(_ + _._2._2.toLong, _ + _) // we need Long because n might exceed the max. Int value


        wordFeatureCountsFiltered
            .join(wordCounts)
            .map({case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc))})
            .join(featureCounts)
            .map({case (feature, ((word, wfc, wc), fc)) => (word, (feature, wc, fc, wfc, n, lmi(n, wc, fc, wfc)))})
            .map({case (word, (feature, wc, fc, wfc, n, lmi)) => word + "\t" + feature + "\t" + wc + "\t" + fc  + "\t" + wfc  + "\t" + n + "\t" + lmi})
            .saveAsTextFile(dir + "__LMI_AllValuesPerWord")

        val featuresPerWordWithScore = wordFeatureCountsFiltered
            .join(wordCounts)
            .map({case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc))})
            .join(featureCounts)
            .map({case (feature, ((word, wfc, wc), fc)) => (word, (feature, lmi(n, wc, fc, wfc)))})
            .filter({case (word, (feature, score)) => score >= param_s})
            .groupByKey()
            // (word, [(feature, score), (feature, score), ...])
            .mapValues(featureScores => featureScores.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2}).take(param_p)) // sort by value desc

        featuresPerWordWithScore
            .flatMap({case (word, featureScores) => for(featureScore <- featureScores) yield (word, featureScore)})
            .map({case (word, (feature, score)) => word + "\t" + feature + "\t" + score})
            .saveAsTextFile(dir + "__LMI_PruneGraph")

        val wordsPerFeatureWithScore = featuresPerWordWithScore
            .flatMap({case (word, featureScores) => for(featureScore <- featureScores) yield (featureScore._1, (word, 1))})
            .groupByKey()
        wordsPerFeatureWithScore.cache()

        wordsPerFeatureWithScore
            .map({case (feature, wordList) => feature + "\t" + wordList.map(f => f._1).mkString("\t")})
            .saveAsTextFile(dir + "__LMI_AggrPerFeature")

        val wordSims = wordsPerFeatureWithScore
            .flatMap({case (feature, wordScores) => for((word1, score1) <- wordScores; (word2, score2) <- wordScores) yield ((word1, word2), score2)})
            .reduceByKey((score1, score2) => score1 + score2)
            .map({case ((word1, word2), score) => (word1, (word2, score))})
            .groupByKey()
            .mapValues(simWords => simWords.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2}).take(param_l))
            .flatMap({case (word, simWords) => for(simWord <- simWords) yield (word, simWord)})

        wordSims.map({case (word1, (word2, sim)) => word1 + "\t" + word2 + "\t" + sim}).saveAsTextFile(dir + "__LMI_Sim")
    }
}

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

object WordAndFeatureCount {
    def ll(n:Int, wc:Int, fc:Int, bc:Int): Double = {
        val wcL = math.log(wc) / math.log(2)
        val fcL = math.log(fc) / math.log(2)
        val bcL = math.log(bc) / math.log(2)
        val res = 2*(n*math.log(n) / math.log(2)
                            -wc*wcL
                            -fc*fcL
                            +bc*bcL
                            +(n-wc-fc+bc)*math.log(n-wc-fc+bc+0.000001) / math.log(2)
                            +(wc-bc)*math.log(wc-bc+0.000001) / math.log(2)
                            +(fc-bc)*math.log(fc-bc+0.000001) / math.log(2)
                            -(n-wc)*math.log(n-wc+0.000001) / math.log(2)
                            -(n-fc)*math.log(n-fc+0.000001) / math.log(2) )
        if ((n*bc)<(wc*fc)) -res else res
    }

    def lmi(n:Int, wc:Int, fc:Int, bc:Int): Double = {
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
            .saveAsTextFile(dir + "__LL_WordCount")

        val _wordsPerFeature = wordFeatureCounts
            .map({case (word, (feature, wfc)) => (feature, word)})
            .groupByKey()

        val wordsPerFeature = _wordsPerFeature
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
            .saveAsTextFile(dir + "__LL_FeatureCount")

        val wordFeatureCountsFiltered = wordFeatureCounts
            .filter({case (word, (feature, wfc)) => wfc >= param_t})
        wordFeatureCountsFiltered.cache()

        val n = wordFeatureCountsFiltered
            .aggregate(0)(_ + _._2._2, _ + _)


        wordFeatureCountsFiltered
            .join(wordCounts)
            .map({case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc))})
            .join(featureCounts)
            .map({case (feature, ((word, wfc, wc), fc)) => (word, (feature, wc, fc, wfc, n, ll(n, wc, fc, wfc)))})
            .map({case (word, (feature, wc, fc, wfc, n, ll)) => word + "\t" + feature + "\t" + wc + "\t" + fc  + "\t" + wfc  + "\t" + n + "\t" + ll})
            .saveAsTextFile("__LL_AllValuesPerWord")

        val featuresPerWordWithScore = wordFeatureCountsFiltered
            .join(wordCounts)
            .map({case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc))})
            .join(featureCounts)
            .map({case (feature, ((word, wfc, wc), fc)) => (word, (feature, ll(n, wc, fc, wfc)))})
            .filter({case (word, (feature, score)) => score >= param_s})
            .groupByKey()
            // (word, [(feature, score), (feature, score), ...])
            .mapValues(featureScores => featureScores.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2}).take(param_p)) // sort by value desc

        featuresPerWordWithScore
            .flatMap({case (word, featureScores) => for(featureScore <- featureScores) yield (word, featureScore)})
            .map({case (word, (feature, score)) => word + "\t" + feature + "\t" + score})
            .saveAsTextFile(dir + "__LL_PruneGraph")

        val wordsPerFeatureWithScore = featuresPerWordWithScore
            .flatMap({case (word, featureScores) => for(featureScore <- featureScores) yield (featureScore._1, (word, 1))})
            .groupByKey()
        wordsPerFeatureWithScore.cache()

        wordsPerFeatureWithScore
            .map({case (feature, wordList) => feature + "\t" + wordList.map(f => f._1).mkString("\t")})
            .saveAsTextFile(dir + "__LL_AggrPerFeature")

        val wordSims = wordsPerFeatureWithScore
            .flatMap({case (feature, wordScores) => for((word1, score1) <- wordScores; (word2, score2) <- wordScores) yield ((word1, word2), score2)})
            .reduceByKey((score1, score2) => score1 + score2)
            .map({case ((word1, word2), score) => (word1, (word2, score))})
            .groupByKey()
            .mapValues(simWords => simWords.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2}).take(param_l))
            .flatMap({case (word, simWords) => for(simWord <- simWords) yield (word, simWord)})

        wordSims.map({case (word1, (word2, sim)) => word1 + "\t" + word2 + "\t" + sim}).saveAsTextFile(dir + "__LL_Sim")
    }
}

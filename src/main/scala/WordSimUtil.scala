import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object WordSimUtil {
    val DEBUG = true

    def log2(n:Double): Double = {
        math.log(n) / math.log(2)
    }

    /**
     * Computes a log-likelihood ratio approximation
     *
     * @param n Total number of observations
     * @param n_A Number of times A was observed
     * @param n_B Number of times B was observed
     * @param n_AB Number of times A and B were observed together
     */
    def ll(n:Long, n_A:Long, n_B:Long, n_AB:Long): Double = {
        val wcL = log2(n_A)
        val fcL = log2(n_B)
        val bcL = log2(n_AB)
        val epsilon = 0.000001
        val res = 2*(n*log2(n)
            -n_A*wcL
            -n_B*fcL
            +n_AB*bcL
            +(n-n_A-n_B+n_AB)*log2(n-n_A-n_B+n_AB+epsilon)
            +(n_A-n_AB)*log2(n_A-n_AB+epsilon)
            +(n_B-n_AB)*log2(n_B-n_AB+epsilon)
            -(n-n_A)*log2(n-n_A+epsilon)
            -(n-n_B)*log2(n-n_B+epsilon) )
        if ((n*n_AB)<(n_A*n_B)) -res.toDouble else res.toDouble
    }

    def descriptivity(n_A:Long, n_B:Long, n_AB:Long): Double = {
        n_AB.toDouble*n_AB.toDouble / (n_A.toDouble*n_B.toDouble)
    }

    /**
     * Computes the lexicographer's mutual information (LMI) score:<br/>
     *
     * <pre>LMI(A,B) = n_AB * log2( (n*n_AB) / (n_A*n_B) )</pre>
     * <br/>
     * Reference:
     * Kilgarri, A., Rychly, P., Smrz, P., Tugwell, D.: The sketch engine. In: <i>Proceedings of Euralex</i>, Lorient, France (2004) 105-116
     * 
     * @param n Total number of observations
     * @param n_A Number of times A was observed
     * @param n_B Number of times B was observed
     * @param n_AB Number of times A and B were observed together
     */
    def lmi(n:Long, n_A:Long, n_B:Long, n_AB:Long): Double = {
        n_AB*(log2(n*n_AB) - log2(n_A*n_B))
    }

    def computeWordFeatureCounts(file:RDD[String],
                                 outDir:String)
    : (RDD[(String, (String, Int))], RDD[(String, Int)], RDD[(String, Int)]) = {
        val wordFeaturesOccurrences = file
            .map(line => line.split("\t"))
            .map({case Array(word, feature, dataset, wordPos, featurePos) => (word, feature, dataset.hashCode, wordPos, featurePos)
        case _ => ("BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE")})
        //wordFeaturesOccurrences.cache()

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

        val featureCounts = wordFeaturesOccurrences
            .map({case (word, feature, dataset, wordPos, featurePos) => ((feature, dataset, featurePos), 1)})
            .reduceByKey((v1, v2) => v1 + v2)
            .map({case ((feature, dataset, featurePos), numOccurrences) => (feature, 1)})
            .reduceByKey((v1, v2) => v1 + v2)
        featureCounts.cache()

        if (DEBUG) {
            wordCounts
                .map({ case (word, count) => word + "\t" + count})
                .saveAsTextFile(outDir + "__WordCount")
            featureCounts
                .map({ case (feature, count) => feature + "\t" + count})
                .saveAsTextFile(outDir + "__FeatureCount")
            wordFeatureCounts
                .map({ case (word, (feature, count)) => word + "\t" + feature + "\t" + count})
                .saveAsTextFile(outDir + "__WordFeatureCount")
        }

        (wordFeatureCounts, wordCounts, featureCounts)
    }

    def computeWordSimsWithFeatures(wordFeatureCounts:RDD[(String, (String, Int))],
                                    wordCounts:RDD[(String, Int)],
                                    featureCounts:RDD[(String, Int)],
                                    w:Int,    // max. number of words per feature
                                    t:Int,    // lower word-feature count threshold
                                    s:Double, // lower significance threshold
                                    p:Int,    // max. number of features per word
                                    l:Int,    // max. number of similar words per word
                                    sig:(Long, Long, Long, Long) => Double,
                                    outDir:String)
    : RDD[(String, (String, Int, Set[String]))] = {

        val wordFeatureCountsFiltered = wordFeatureCounts
            .filter({case (word, (feature, wfc)) => wfc >= t})
        wordFeatureCountsFiltered.cache()

        val wordsPerFeature = wordFeatureCountsFiltered
            .map({case (word, (feature, wfc)) => (feature, word)})
            .groupByKey()
            .mapValues(v => v.size)
            .filter({case (feature, numWords) => numWords <= w})

        val featureCountsFiltered = featureCounts
            .filter({case (feature, fc) => fc >= t})
            .join(wordsPerFeature) // filter by using a join
            .map({case (feature, (fc, fwc)) => (feature, fc)}) // and remove unnecessary data from join
        featureCountsFiltered.cache()

        val wordCountsFiltered = wordCounts
            .filter({case (word, wc) => wc >= t})
        wordCountsFiltered.cache()

        // Since word counts and feature counts are based on unfiltered word-feature
        // occurrences, n must be based on unfiltered word-feature counts as well.
        val n = wordFeatureCounts
            .map({case (word, (feature, wfc)) => (feature, (word, wfc))})
            .aggregate(0L)(_ + _._2._2.toLong, _ + _) // we need Long because n might exceed the max. Int value

        val featuresPerWordWithScore = wordFeatureCountsFiltered
            .join(wordCountsFiltered)
            .map({case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc))})
            .join(featureCountsFiltered)
            .map({case (feature, ((word, wfc, wc), fc)) => (word, (feature, sig(n, wc, fc, wfc)))})
            .filter({case (word, (feature, score)) => score >= s})
            .groupByKey()
            // (word, [(feature, score), (feature, score), ...])
            .mapValues(featureScores => featureScores.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2}).take(p)) // sort by value desc
        featuresPerWordWithScore.cache()

        val featuresPerWord:RDD[(String, Array[String])] = featuresPerWordWithScore
            .map({case (word, featureScores) => (word, featureScores.map({case (feature, score) => feature}))})

        val wordsPerFeatureWithScore = featuresPerWordWithScore
            .flatMap({case (word, featureScores) => for(featureScore <- featureScores) yield (featureScore._1, (word, 1))})
            .groupByKey()
        wordsPerFeatureWithScore.cache()

        val wordSims:RDD[(String, (String, Int))] = wordsPerFeatureWithScore
            .flatMap({case (feature, wordScores) => for((word1, score1) <- wordScores; (word2, score2) <- wordScores) yield ((word1, word2), score2)})
            .reduceByKey((score1, score2) => score1 + score2)
            .map({case ((word1, word2), score) => (word1, (word2, score))})
            .groupByKey()
            .mapValues(simWords => simWords.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2}).take(l))
            .flatMap({case (word, simWords) => for(simWord <- simWords) yield (word, simWord)})

        val wordSimsWithFeatures:RDD[(String, (String, Int, Set[String]))] = wordSims
            .join(featuresPerWord)
            .map({case (word, ((simWord, score), featureList1)) => (simWord, (word, score, featureList1))})
            .join(featuresPerWord)
            .map({case (simWord, ((word, score, featureList1), featureList2)) => (word, (simWord, score, featureList1.toSet.intersect(featureList2.toSet)))})

        if (DEBUG) {
            featuresPerWordWithScore
                .flatMap({ case (word, featureScores) => for (featureScore <- featureScores) yield (word, featureScore)})
                .map({ case (word, (feature, score)) => word + "\t" + feature + "\t" + score})
                .saveAsTextFile(outDir + "__PruneGraph")
            wordFeatureCountsFiltered
                .join(wordCountsFiltered)
                .map({ case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc))})
                .join(featureCountsFiltered)
                .map({ case (feature, ((word, wfc, wc), fc)) => word + "\t" + feature + "\t" + wc + "\t" + fc + "\t" + wfc + "\t" + n + "\t" + sig(n, wc, fc, wfc)})
                .saveAsTextFile(outDir + "__AllValuesPerWord")
            wordsPerFeatureWithScore
                .map({ case (feature, wordList) => feature + "\t" + wordList.map(f => f._1).mkString("\t")})
                .saveAsTextFile(outDir + "__AggrPerFeature")
        }

        wordSimsWithFeatures
    }

}

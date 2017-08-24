package de.uhh.lt.jst.dt

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD

object WordSimLib {
  val DEBUG = false

  def computeWordFeatureCounts(file: RDD[String],
                               outDir: String)
  : (RDD[(String, (String, Int))], RDD[(String, Int)], RDD[(String, Int)]) = {
    val wordFeaturesOccurrences = file
      .map(line => line.split("\t"))
      .map {
        case Array(word, feature, dataset, wordPos, featurePos) => (word, feature, dataset.hashCode, wordPos, featurePos)
        case _ => ("BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE")
      }

    val wordFeatureCounts = wordFeaturesOccurrences
      .map({ case (word, feature, dataset, wordPos, featurePos) => ((word, feature, dataset, wordPos, featurePos), 1) })
      .reduceByKey((v1, v2) => v1 + v2) // count same occurences only once (make them unique)
      .map({ case ((word, feature, dataset, wordPos, featurePos), numOccurrences) => ((word, feature), 1) })
      .reduceByKey((v1, v2) => v1 + v2)
      .map({ case ((word, feature), count) => (word, (feature, count)) })
    wordFeatureCounts.cache()

    val wordCounts = wordFeaturesOccurrences
      .map({ case (word, feature, dataset, wordPos, featurePos) => ((word, dataset, wordPos), 1) })
      .reduceByKey((v1, v2) => v1 + v2)
      .map({ case ((word, dataset, wordPos), numOccurrences) => (word, 1) })
      .reduceByKey((v1, v2) => v1 + v2)
    wordCounts.cache()

    val featureCounts = wordFeaturesOccurrences
      .map({ case (word, feature, dataset, wordPos, featurePos) => ((feature, dataset, featurePos), 1) })
      .reduceByKey((v1, v2) => v1 + v2)
      .map({ case ((feature, dataset, featurePos), numOccurrences) => (feature, 1) })
      .reduceByKey((v1, v2) => v1 + v2)
    featureCounts.cache()

    if (DEBUG) {
      wordCounts
        .map({ case (word, count) => word + "\t" + count })
        .saveAsTextFile(outDir + "/WordCount")
      featureCounts
        .map({ case (feature, count) => feature + "\t" + count })
        .saveAsTextFile(outDir + "/FeatureCount")
      wordFeatureCounts
        .map({ case (word, (feature, count)) => word + "\t" + feature + "\t" + count })
        .saveAsTextFile(outDir + "/WordFeatureCount")
    }

    (wordFeatureCounts, wordCounts, featureCounts)
  }

  def computeFeatureScores(wordFeatureCounts: RDD[(String, (String, Int))],
                           wordCounts: RDD[(String, Int)],
                           featureCounts: RDD[(String, Int)],
                           outputDir: String,
                           wordsPerFeatureNum: Int,
                           featuresPerWordNum: Int,
                           wordCountMin: Int,
                           featureCountMin: Int,
                           wordFeatureCountMin: Int,
                           significanceMin: Double,
                           significance: (Long, Long, Long, Long) => Double) = {

    val wordFeatureCountsFiltered =
      if (wordFeatureCountMin > 1) wordFeatureCounts.filter({ case (word, (feature, wfc)) => wfc >= wordFeatureCountMin })
      else wordFeatureCounts

    var featureCountsFiltered =
      if (featureCountMin > 1) featureCounts.filter({ case (feature, fc) => fc >= featureCountMin })
      else featureCounts

    val wordsPerFeatureCounts = wordFeatureCountsFiltered
      .map { case (word, (feature, wfc)) => (feature, word) }
      .groupByKey()
      .mapValues(v => v.size)
      .filter { case (feature, numWords) => numWords <= wordsPerFeatureNum }

    featureCountsFiltered = featureCountsFiltered
      .join(wordsPerFeatureCounts) // filter using a join
      .map { case (feature, (fc, fwc)) => (feature, fc) } // remove unnecessary data from join
    featureCountsFiltered.cache()

    val wordCountsFiltered =
      if (wordCountMin > 1) wordCounts.filter({ case (word, wc) => wc >= wordCountMin })
      else wordCounts
    wordCountsFiltered.cache()

    // Since word counts and feature counts are based on unfiltered word-feature
    // occurrences, n must be based on unfiltered word-feature counts as well
    val n = wordFeatureCounts
      .map({ case (word, (feature, wfc)) => (feature, (word, wfc)) })
      .aggregate(0L)(_ + _._2._2.toLong, _ + _) // we need Long because n might exceed the max. Int value

    val featuresPerWordWithScore = wordFeatureCountsFiltered
      .join(wordCountsFiltered)
      .map({ case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc)) })
      .join(featureCountsFiltered)
      .map({ case (feature, ((word, wfc, wc), fc)) => (word, (feature, significance(n, wc, fc, wfc))) })
      .filter({ case (word, (feature, score)) => score >= significanceMin })
      .groupByKey()
      // (word, [(feature, score), (feature, score), ...])
      .mapValues(featureScores => featureScores.toArray.sortWith({ case ((_, s1), (_, s2)) => s1 > s2 }).take(featuresPerWordNum)) // sort by value desc

    if (DEBUG) {
      wordFeatureCountsFiltered
        .join(wordCountsFiltered)
        .map({ case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc)) })
        .join(featureCountsFiltered)
        .map({ case (feature, ((word, wfc, wc), fc)) => (word, feature, wc, fc, wfc, significance(n, wc, fc, wfc)) })
        .sortBy({ case (word, feature, wc, fc, wfc, score) => score }, ascending = false)
        .map({ case (word, feature, wc, fc, wfc, score) => word + "\t" + feature + "\t" + wc + "\t" + fc + "\t" + wfc + "\t" + n + "\t" + score })
        .saveAsTextFile(outputDir + "/AllValuesPerWord")
    }

    featuresPerWordWithScore
  }

  def getSignificance(significanceType: String) = {
    significanceType match {
      case "LMI" => SimMeasures.lmi _
      case "COV" => SimMeasures.cov _
      case "FREQ" => SimMeasures.freq _
      case _ => SimMeasures.ll _
    }
  }

  def computeWordSimsWithFeatures(wordFeatureCounts: RDD[(String, (String, Int))],
                                  wordCounts: RDD[(String, Int)],
                                  featureCounts: RDD[(String, Int)],
                                  outputDir: String,
                                  wordsPerFeatureMax: Int,
                                  featuresPerWordMaxp: Int,
                                  wordCountMin: Int,
                                  featureCountMin: Int,
                                  wordFeatureCountMin: Int,
                                  significanceMin: Double,
                                  similarWordsMaxNum: Int,
                                  significanceType: String) = {

    val wordSimsPath = outputDir + "/SimPruned"
    val wordSimsPrunedWithFeaturesPath = if (DEBUG) outputDir + "/SimPrunedWithFeatures" else ""
    val featuresPath = outputDir + "/FeaturesPruned"

    // Normalize and prune word features
    val sig = getSignificance(significanceType)
    val featuresPerWordWithScore = computeFeatureScores(wordFeatureCounts, wordCounts, featureCounts, outputDir, wordsPerFeatureMax, featuresPerWordMaxp, wordCountMin, featureCountMin, wordFeatureCountMin, significanceMin, sig)
    featuresPerWordWithScore.cache()

    featuresPerWordWithScore
      .flatMap { case (word, featureScores) => (featureScores
        .map { case (feature, score) => f"$word\t$feature\t$score%.5f" })
      }
      .saveAsTextFile(featuresPath, classOf[GzipCodec])

    // Compute word similarities
    val featuresPerWord: RDD[(String, Array[String])] = featuresPerWordWithScore
      .map { case (word, featureScores) => (word, featureScores
        .map { case (feature, score) => feature })
      }

    val wordsPerFeature = featuresPerWord
      .flatMap({ case (word, features) => for (feature <- features.iterator) yield (feature, word) })
      .groupByKey()
      .filter({ case (feature, words) => words.size <= wordsPerFeatureMax })
      .sortBy(_._2.size, ascending = false)

    val wordsPerFeatureFairPartitioned = wordsPerFeature
      // the following 4 lines partition the RDD for equal words-per-feature distribution over the partitions
      .zipWithIndex()
      .map { case ((feature, words), index) => (index, (feature, words)) }
      .partitionBy(new de.uhh.lt.jst.utils.IndexModuloPartitioner(1000))
      .map { case (index, (feature, words)) => (feature, words) }
    wordsPerFeatureFairPartitioned.cache()

    val wordSimsAll: RDD[(String, (String, Double))] = wordsPerFeatureFairPartitioned
      .flatMap { case (feature, words) => for (word1 <- words.iterator; word2 <- words.iterator) yield ((word1, word2), 1.0) }
      .reduceByKey { case (score1, score2) => score1 + score2 }
      .map { case ((word1, word2), scoreSum) => (word1, (word2, (scoreSum / featuresPerWordMaxp).toDouble)) }
      .sortBy({ case (word, (simWord, score)) => (word, score) }, ascending = false)

    val wordSimsPruned: RDD[(String, (String, Double))] = wordSimsAll
      .groupByKey()
      .mapValues(simWords => simWords.toArray
        .sortWith { case ((w1, s1), (w2, s2)) => s1 > s2 }
        .take(similarWordsMaxNum))
      .flatMap { case (word, simWords) => for (simWord <- simWords.iterator) yield (word, simWord) }
      .cache()

    wordSimsPruned
      .map { case (word1, (word2, score)) => f"$word1\t$word2\t$score%.5f" }
      .saveAsTextFile(wordSimsPath, classOf[GzipCodec])

    if (DEBUG) {
      wordsPerFeature
        .map { case (feature, words) => s"$feature\t${words.size}\t${words.mkString("  ")}" }
        .saveAsTextFile(outputDir + "/WordsPerFeature", classOf[GzipCodec])

      wordSimsPruned
        .join(featuresPerWord)
        .map({ case (word, ((simWord, score), featureList1)) => (simWord, (word, score, featureList1)) })
        .join(featuresPerWord)
        .map({ case (simWord, ((word, score, featureList1), featureList2)) => (word, (simWord, score, featureList1.toSet.intersect(featureList2.toSet))) })
        .sortBy({ case (word, (simWord, score, mutualFeatureSet)) => (word, score) }, ascending = false)
        .map { case (word1, (word2, score, featureSet)) => f"$word1\t$word2\t$score%.5f\t${featureSet.toList.sorted.mkString("  ")}" }
        .saveAsTextFile(wordSimsPrunedWithFeaturesPath, classOf[GzipCodec])
    }

    (wordSimsPath, wordSimsPrunedWithFeaturesPath, featuresPath)
  }

}

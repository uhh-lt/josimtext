import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import scala.util.Try

object ClueAggregator {
  val MAX_SIM_WORDS_NUM = 20
  val MAX_FEATURE_NUM = 10000
  val MIN_WORD_FEATURE_COUNT = 0.0
  val FEATURE_TYPE = "words"  // "deps" or "words-from-deps"
  val LOWERCASE_WORDS_FROM_DEPS = true

  val stopwords = Util.getStopwords()

  def keepFeature(feature:String, featureType:String) = {
    if (featureType == "deps" || featureType == "words-from-deps"){
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

  def transformFeature(feature:String, featureType:String, lowercase:Boolean=LOWERCASE_WORDS_FROM_DEPS) = {
    var res: String = ""
    if (featureType == "words-from-deps"){
      val (depType, srcWord, dstWord) = Util.parseDep(feature)
      if (srcWord == Const.HOLE || srcWord == Const.HOLE_DEPRECATED) {
        res = dstWord
      } else{
        res = srcWord
      }
    } else {
      res = feature
    }

    res = if (lowercase) res.trim().toLowerCase() else res.trim()
    res
  }

  def formatFeatures(featureList: List[(String, Double)], maxFeatureNum:Int, featureType:String, target:String) = {
    val filteredFeatureList = featureList
      .filter({ case (feature, prob) => keepFeature(feature, featureType) })
      .map({ case (feature, prob) => (transformFeature(feature, featureType), prob) })

    if (featureType == "words-from-deps") {
      filteredFeatureList
        .groupBy(_._1)
        .map({case (feature, probList) => (feature, probList.map({case (feature, prob) => prob}).sum)})
        .filterKeys(_ != target)
        .toList
        .sortBy(-_._2)
        .take(maxFeatureNum)
    } else{
      filteredFeatureList
        .take(maxFeatureNum)
    }
  }

  def main(args: Array[String]) {
    if (args.length < 5) {
      println("Aggregates clues of word sense cluster.")
      println("Usage: ClusterContextClueAggregator <senses> <word-counts> <feature-counts> <word-feature-counts> <output> [cluser-words-num] [dependency-features] [max-feature-num] [min-word-feature-count] [target-words]")
      println("Example: /Users/alex/Desktop/debug/ruby-java/senses-1k.csv-voc /Users/alex/Desktop/debug/ruby-java/W-voc /Users/alex/Desktop/debug/ruby-java/F-voc /Users/alex/Desktop/debug/ruby-java/WF-voc /Users/alex/Desktop/debug/ruby-java/senses-1k.csv-voc-clues-new3 200 words-from-deps 20 2 /")
      return
    }

    // Initialization
    val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val sensesPath = args(0)
    val wordsPath = args(1)
    val featuresPath = args(2)
    val wordFeaturesPath = args(3)
    val outputPath = args(4)
    val numSimWords = if (args.length > 5) args(5).toInt else MAX_SIM_WORDS_NUM
    var featureType = if (args.length > 6) args(6) else FEATURE_TYPE
    if (!Set("words", "deps", "words-from-deps").contains(featureType)) {
      println("Warning: wrong feature type. Using 'words' feature type.")
      featureType = "words"
    }
    val maxFeatureNum = if (args.length > 7) args(7).toInt else MAX_FEATURE_NUM
    val minWordFeatureCount = if (args.length > 8) args(8).toDouble else MIN_WORD_FEATURE_COUNT
    val targetWords:Set[String] = if (args.length > 9) Util.loadVocabulary(sc, args(9)) else Set()

    println("Senses: " + sensesPath)
    println("Words: " + wordsPath)
    println("Word features: " + wordFeaturesPath)
    println("Features: " + featuresPath)
    println("Output: " + outputPath)
    println("Number of similar words from cluster: " + numSimWords)
    println("Feature type: " + featureType)
    println("Maximum number of features: " + maxFeatureNum)
    println("Minimum word feature count: " + minWordFeatureCount)
    println("Target words: " + targetWords)

    Util.delete(outputPath)


    // Load W, F, WF

    val wordCounts:RDD[(String, Long)] = sc  // word, freq
      .textFile(wordsPath)
      .map(line => line.split("\t"))
      .map({case Array(word, freq) => (word, freq.toLong)})

    val featureCounts:RDD[(String, Long)] = sc  // feature, freq
      .textFile(featuresPath)
      .map(line => line.split("\t"))
      .map({case Array(word, freq) => (word, freq.toLong)})

    val wordFeatureCounts = sc  // word, (feature, wc, fc, wfc)
      .textFile(wordFeaturesPath)
      .map(line => line.split("\t"))
      .map(cols => (cols(1), (cols(0), cols(2).toLong))) // (feature, (word, wfc))
      .filter({case (feature, (word, wfc)) => wfc >= minWordFeatureCount})
      .join(featureCounts)
      .map({case (feature, ((word, wfc), fc)) => (word, (feature, wfc, fc))})
      .join(wordCounts)
      .map({case (word, ((feature, wfc, fc), wc)) => (word, (feature, wc, fc, wfc))})

    val senseClusters = sc  // (target, sense_id), [((word, sim), sum_sum)]
      .textFile(sensesPath)
      .map(line => line.split("\t"))
      .map({case Array(target, sense_id, keyword, cluster) => (target, sense_id, keyword, cluster) case _ => ("?", "-1", "?", "") })
      .filter({case (target, sense_id, keyword, cluster) => target != "?"})
      .map({case (target, sense_id, keyword, cluster) => (
        (target, sense_id),
        cluster.split(Const.LIST_SEP)
          .take(numSimWords)
          .map(wordWithSim => Util.splitLastN(wordWithSim, Const.SCORE_SEP, 2))
          .map({case Array(word, sim) =>  if (Try(sim.toDouble).isSuccess) (word.trim(), sim.toDouble) else (word.trim(), 0.0) case _ => ("?", 0.0)})
          .filter({ case (word, sim) => !stopwords.contains(word) })   )})
      .filter({case ((target, sense_id), simWords) => targetWords.size == 0 || targetWords.contains(target)})
      .map({case ((target, sense_id), simWords) => ((target, sense_id), (simWords, simWords.map(_._2).sum))})
      .cache()

    val clusterWords = senseClusters  // clusterword, (target, sim, sense_id, sim_sum)
      .flatMap({case ((target, sense_id), (simWordsWithSim, simSum)) => for((clusterWord, sim) <- simWordsWithSim) yield (clusterWord, (target, sim, sense_id, simSum))})


    val clusterWordsFeatures: RDD[(String, ((String, Double, String, Double), (String, Long, Long, Long)))] = clusterWords
      .join(wordFeatureCounts)
      .cache()


    clusterWordsFeatures.saveAsTextFile(outputPath)
    println("lakjsf")
    //val senseFetures = clusterWordsFeatures.flatMap{case (clusterword, ((target, sim, sense_id, simSum), (feature, wc, fc, wfc))) => target}
    // so far very fast


  }
}

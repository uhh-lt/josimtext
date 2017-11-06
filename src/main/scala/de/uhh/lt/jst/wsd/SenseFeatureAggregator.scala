package de.uhh.lt.jst.wsd

import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.utils.{Const, Util}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import scopt.Read
import scopt.Read.reads

import scala.util.Try

object SenseFeatureAggregator  extends SparkJob {

  object FeatureType extends Enumeration {
    type FeatureType = Value
    val words, deps, depwords, trigrams = Value
  }
  import FeatureType._


  object FeatureNorm extends Enumeration {
    type FeatureNorm = Value
    val wc, lmi = Value
  }
  import FeatureNorm.FeatureNorm


  val PRIOR_FEATURE_PROB = 0.000001
  val LOWERCASE_WORDS_FROM_DEPS = true
  val MIN_WORD_LEN = 3
  val _stopwords = Util.getStopwords()
  val _numbersRegex = """\d+""".r

  case class Config(
    sensesPath: String = "",
    wordsPath: String = "",
    featuresPath: String = "",
    wordFeaturesPath: String = "",
    outputPath: String = "",
    numSimWords: Int = 50,
    featureType: FeatureType = words,
    maxFeatureNum: Int = 10000,
    minWordFeatureCount: Int = 0,
    featureScoreNorm: FeatureNorm = FeatureNorm.lmi,
    vocFile: Option[String] = None
  )

  override type ConfigType = Config
  override val config = Config()
  override val description = "Aggregates clues of word sense cluster."
  override val parser = new Parser {

    implicit val readFeatureType: Read[FeatureType] = reads { FeatureType.withName }
    implicit val readFeatureNorm: Read[FeatureNorm.FeatureNorm] = reads { FeatureNorm.withName }

    arg[String]("SENSES_FILE").action( (x, c) =>
      c.copy(sensesPath = x) ).required().hidden()

    arg[String]("WORDS_FILE").action( (x, c) =>
      c.copy(wordsPath = x) ).required().hidden()

    arg[String]("FEATURES_FILE").action( (x, c) =>
      c.copy(featuresPath = x) ).required().hidden()

    arg[String]("WORD_FEATURES_FILE").action( (x, c) =>
      c.copy(wordFeaturesPath = x) ).required().hidden()

    arg[String]("OUTPUT_FOLDER").action( (x, c) =>
      c.copy(outputPath = x) ).required().hidden()

    opt[Int]('w', "numSimWords").action( (x, c) =>
      c.copy(numSimWords = x) ).
      text(s"Number of similar words. (default: ${config.numSimWords}")

    opt[FeatureType]('t', "featureType").action( (x, c) =>
      c.copy(featureType = x) ).
      text(s"Feature type: ${FeatureType.values.mkString(", ")}. (default ${config.featureType})")

    opt[Int]('f', "maxFeatureNum").action( (x, c) =>
      c.copy(maxFeatureNum = x) ).
      text(s"Maximal number of features. (default ${config.maxFeatureNum}")

    opt[FeatureNorm]('s', "featureScoreNorm").action( (x, c) =>
      c.copy(featureScoreNorm = x) ).
      text(s"Feature norm for scoring: ${FeatureNorm.values.mkString(", ")}. (default ${config.featureScoreNorm})")

    opt[String]('v', "vocFile").action( (x, c) =>
      c.copy(vocFile = Some(x)) ).
      text(s"Vocabulary file of target words")
  }

  def keepFeature(feature: String, featureType: FeatureType): Boolean = {
    if (featureType == deps || featureType == depwords) {
      // dependency features
      val (depType, srcWord, dstWord) = Util.parseDep(feature)
      if (Const.Resources.STOP_DEPENDENCIES.contains(depType) || _stopwords.contains(srcWord) || _stopwords.contains(dstWord)) {
        false
      } else {
        true
      }
    } else if (featureType == trigrams) {
      val (left, right) = Util.parseTrigram(feature)
      val noNumbers = !_numbersRegex.findFirstIn(left).isDefined && !_numbersRegex.findFirstIn(right).isDefined
      val leftOK = !(_stopwords.contains(left) || left.length == 1)
      val rightOK = !(_stopwords.contains(right) || right.length == 1)
      noNumbers && (leftOK || rightOK)
    } else { // word features, in contrast to dependency features
      val featureTrim = feature.trim()
      !_stopwords.contains(featureTrim) && !_stopwords.contains(featureTrim.toLowerCase())
    }
  }

  def transformFeature(feature: String, featureType: FeatureType, lowercase: Boolean = LOWERCASE_WORDS_FROM_DEPS): String = {
    var res: String = ""
    if (featureType == depwords) {
      val (depType, srcWord, dstWord) = Util.parseDep(feature)
      if (srcWord == Const.HOLE || srcWord == Const.HOLE_DEPRECATED) {
        res = dstWord
      } else {
        res = srcWord
      }
    } else {
      res = feature
    }

    res = if (lowercase) res.trim().toLowerCase() else res.trim()
    res
  }

  def formatFeatures(
                      featureList: List[(String, Double)],
                      maxFeatureNum: Int,
                      featureType: FeatureType,
                      target: String
                    ): List[(String, Double)] = {

    val filteredFeatureList = featureList
      .filter { case (feature, prob) => keepFeature(feature, featureType) }
      .map { case (feature, prob) => (transformFeature(feature, featureType), prob) }
      .sortBy(_._2)
      .reverse

    if (featureType == depwords) {
      // this feature after transformation will be just a word
      filteredFeatureList
        .groupBy(_._1)
        .map({ case (feature, probList) => (feature, probList.map({ case (feature, prob) => prob }).sum) })
        .filterKeys(_ != target)
        .toList
        .sortBy(-_._2)
        .take(maxFeatureNum)
    } else {
      filteredFeatureList
        .take(maxFeatureNum)
    }
  }

  def featureScore(sim: Double, wc: Long, fc: Long, wfc: Long, featureScoreNorm: FeatureNorm): Double = {
    if (featureScoreNorm == FeatureNorm.lmi) {
      if (wc.toDouble * fc.toDouble == 1) {
        PRIOR_FEATURE_PROB
      } else {
        -sim * wfc * 1 / math.log(wfc.toDouble / (wc.toDouble * fc.toDouble))
      }
    } else {
      sim * wfc.toDouble / wc.toDouble
    }
  }

  def run(sc: SparkContext, config: Config): Unit = {

    val targetWords = config.vocFile
      .map(Util.loadVocabulary(sc, _))
      .getOrElse(Set())

    // Action
    val clusterSimWords: RDD[((String, String), (Array[(String, Double)], Double))] = sc // (target, sense_id), [((word, sim), sum_sum)]
      .textFile(config.sensesPath)
      .map(line => line.split("\t"))
      .map { case Array(target, sense_id, cluster) => (target, sense_id, cluster)
      case Array(target, sense_id) => (target, sense_id, "")
      case _ => ("?", "-1", "")
      }
      .filter { case (target, sense_id, cluster) => target != "?" }
      .map { case (target, sense_id, cluster) => (
        (target, sense_id),
        cluster.split(Const.LIST_SEP)
          .take(config.numSimWords)
          .map(wordWithSim => Util.splitLastN(wordWithSim, Const.SCORE_SEP, 2))
          .map { case Array(word, sim) => if (Try(sim.toDouble).isSuccess) (word.trim(), sim.toDouble) else (word.trim(), 0.0) case _ => ("?", 0.0) }
          .filter({ case (word, sim) => !_stopwords.contains(word) && !_stopwords.contains(word.toLowerCase) && word.length >= MIN_WORD_LEN }))
      }
      .filter { case ((word, sense), simWords) => targetWords.size == 0 || targetWords.contains(word) }
      .map { case ((word, sense), simWords) => ((word, sense), (simWords, simWords.map(_._2).sum)) }
      .cache()

    val clusterSimSums: RDD[((String, String), Double)] = clusterSimWords // (target, sense_id), sum_sum
      .map({ case ((word, sense), (simWords, simSum)) => ((word, sense), simSum) })
      .cache()

    val wordCounts: RDD[(String, Long)] = sc // word, freq
      .textFile(config.wordsPath)
      .map(line => line.split("\t"))
      .map({ case Array(word, freq) => (word, freq.toLong) })

    val featureCounts: RDD[(String, Long)] = sc // feature, freq
      .textFile(config.featuresPath)
      .map(line => line.split("\t"))
      .map({ case Array(word, freq) => (word, freq.toLong) })

    val clusterWords: RDD[(String, (String, Double, String, Double))] = clusterSimWords // word, (target, sim, sense_id, sim_sum)
      .flatMap({ case ((word, sense), (simWordsWithSim, simSum)) => for ((simWord, sim) <- simWordsWithSim) yield (simWord, (word, sim, sense, simSum)) })

    val wordFeatures: RDD[(String, (String, Long, Long, Long))] = sc // word, (feature, wc, fc, wfc)
      .textFile(config.wordFeaturesPath)
      .map(line => line.split("\t"))
      .map(cols => (cols(1), (cols(0), cols(2).toLong))) // (feature, (word, wfc))
      .filter({ case (feature, (word, wfc)) => wfc >= config.minWordFeatureCount })
      .join(featureCounts)
      .map({ case (feature, ((word, wfc), fc)) => (word, (feature, wfc, fc)) })
      .join(wordCounts)
      .map({ case (word, ((feature, wfc, fc), wc)) => (word, (feature, wc, fc, wfc)) })

    val result: RDD[((String, String), (List[(String, Double)], (Array[(String, Double)], Double)))] = clusterWords
      .join(wordFeatures)
      .map { case (simWord, ((word, sim, sense, simSum), (feature, wc, fc, wfc))) => ((word, sense, feature), featureScore(sim, wc, fc, wfc, config.featureScoreNorm)) }
      .reduceByKey(_ + _)
      .map { case ((word, sense, feature), pSum) => ((word, sense), (feature, pSum)) }
      .join(clusterSimSums)
      .map { case ((word, sense), ((feature, pSum), simSum)) => ((word, sense), (feature, pSum / simSum)) }
      .groupByKey()
      .map { case ((word, sense), senseFeatureProbs) => ((word, sense), senseFeatureProbs.toList) }
      .join(clusterSimWords)

    clusterSimWords
      .leftOuterJoin(result)
      .map { case ((word, sense), ((simWordSim, simSum), senseFeatures)) => ((word, sense), senseFeatures.getOrElse((List[(String, Double)](), (simWordSim, 0.0)))) }
      .sortByKey()
      .map { case ((word, sense), (senseFeatureProbs, (simWordSim, simSum))) =>
        word + "\t" +
          sense + "\t" +
          simWordSim
            .map({ case (simWord, sim) => "%s%s%.6f".format(simWord, Const.SCORE_SEP, sim) })
            .mkString(Const.LIST_SEP) + "\t" +
          formatFeatures(senseFeatureProbs, config.maxFeatureNum, config.featureType, word)
            .map({ case (feature, prob) => "%s%s%.6f".format(feature, Const.SCORE_SEP, prob) })
            .mkString(Const.LIST_SEP)
      }

      .saveAsTextFile(config.outputPath)
  }
}

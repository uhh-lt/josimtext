package de.uhh.lt.jst.wsd

import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.dt.{FreqFilter, WordFeatureFilter}
import de.uhh.lt.jst.utils.Util
import org.apache.spark.{SparkConf, SparkContext}


object SenseFeatureFilter  extends SparkJob {

  case class Config(
    vocFile: String = "",
    senseFile: String = "",
    wordsFile: String = "",
    wordFeaturesFile: String = "",
    featuresFile: String = ""
  )

  override type ConfigType = Config
  override val config = Config()
  override val description = "Creates words, features and word-features of the input vocabulary words."
  override val parser = new Parser {

    arg[String]("VOC_FILE").action( (x, c) =>
      c.copy(vocFile = x) ).required().hidden()

    arg[String]("SENSES_FILE").action( (x, c) =>
      c.copy(senseFile = x) ).required().hidden()

    arg[String]("WORDS_FILE").action( (x, c) =>
      c.copy(wordsFile = x) ).required().hidden()

    arg[String]("WORD_FEATURES_FILE").action( (x, c) =>
      c.copy(wordFeaturesFile = x) ).required().hidden()

    arg[String]("FEATURES_FILE").action( (x, c) =>
      c.copy(featuresFile = x) ).required().hidden()

  }

  val POSTFIX = "-voc"

  def run(sc: SparkContext, config: Config): Unit = {

    val sensesOutPath = config.senseFile + POSTFIX
    val wordsOutPath = config.wordsFile + POSTFIX
    val wordFeaturesOutPath = config.wordFeaturesFile + POSTFIX
    val featuresOutPath = config.featuresFile + POSTFIX

    val voc = Util.loadVocabulary(sc, config.vocFile)
    val (senses, clusterVocRDD) = SensesFilter.run(config.senseFile, voc, sc)
    senses
      .map({ case (target, sense_id, cluster) => target + "\t" + sense_id + "\t" + cluster })
      .saveAsTextFile(sensesOutPath)

    val clusterVoc = clusterVocRDD.collect().toSet.union(voc)
    val words = FreqFilter.calculate(sc, config.wordsFile, clusterVoc, keepSingleWords = false)
    words
      .map({ case (word, freq) => word + "\t" + freq })
      .saveAsTextFile(wordsOutPath)

    val (wordFeatures, featureVocRDD) = WordFeatureFilter.calculate(config.wordFeaturesFile, clusterVoc, sc)
    val featureVoc = featureVocRDD.collect().toSet
    wordFeatures
      .map({ case (word, feature, freq) => word + "\t" + feature + "\t" + freq })
      .saveAsTextFile(wordFeaturesOutPath)

    val features = FreqFilter.calculate(sc, config.featuresFile, featureVoc, keepSingleWords = false)
    features
      .map({ case (feature, freq) => feature + "\t" + freq })
      .saveAsTextFile(featuresOutPath)
  }
}
package de.uhh.lt.jst.dt

import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.dt.WordSimLib.WordSimParameters
import org.apache.spark.sql.SparkSession

object WordSimFromCounts extends SparkJob {

  case class Config(
    wordCountsCSV: String = "",
    featureCountsCSV: String = "",
    wordFeatureCountsCSV: String = "",
    outputDir: String = "",
    parameters: WordSimParameters = WordSimParameters()
  )

  type ConfigType = Config
  override val config = Config()

  override val command: String = "WordSimFromCounts"
  override val description = "Compute a Distributional Thesaurus from word, feature and word-feature frequency CSV files."

  override val parser = new Parser {

    arg[String]("WORD_COUNT_FILE").action( (x, c) =>
      c.copy(wordCountsCSV = x) ).required().hidden()

    arg[String]("FEATURE_COUNT_FILE").action( (x, c) =>
      c.copy(featureCountsCSV = x) ).required().hidden()

    arg[String]("WORD_FEATURE_COUNT_FILE").action( (x, c) =>
      c.copy(wordFeatureCountsCSV = x) ).required().hidden()

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDir = x) ).required().hidden()

    opt[Int]("wpf").action( (x, c) =>
      c.copy(parameters = c.parameters.copy(wordsPersFeature = x))).
      valueName("integer").
      text(s"Number of words per features (default ${config.parameters.wordsPersFeature})")

    opt[Int]("fpw").action( (x, c) =>
      c.copy(parameters = c.parameters.copy(featuresPerWord = x))).
      valueName("integer").
      text(s"Number of features per word (default ${config.parameters.featuresPerWord})")

    opt[Int]("minw").action( (x, c) =>
      c.copy(parameters = c.parameters.copy(minWordCount = x))).
      valueName("integer").
      text(s"Minimum word count (default ${config.parameters.minWordCount})")

    opt[Int]("minf").action( (x, c) =>
      c.copy(parameters = c.parameters.copy(minFeatureCount = x))).
      valueName("integer").
      text(s"Minimum feature count (default ${config.parameters.minFeatureCount})")

    opt[Int]("minwf").action( (x, c) =>
      c.copy(parameters = c.parameters.copy(minWordFeatureCount = x))).
      valueName("integer").
      text(s"Minimum word feature count (default ${config.parameters.minWordFeatureCount})")

    opt[Double]("minsign").action( (x, c) =>
      c.copy(parameters = c.parameters.copy(minSignificance = x))).
      valueName("double").
      text(s"Minimum significance measure (default ${config.parameters.minSignificance})")

    opt[String]("sign").action( (x, c) =>
      c.copy(parameters = c.parameters.copy(significanceType = x))).
      valueName("string").
      text(s"Set the significance measures (LMI, COV, FREQ) (default ${config.parameters.significanceType})")

    // Sometimes this seems to be called NearestNeighboursNum, TODO is using the nnn abbr. good?
    opt[Int]("nnn").action( (x, c) =>
      c.copy(parameters = c.parameters.copy(maxSimilarWords = x))).
      valueName("integer").
      text(s"Number of nearest neighbours, .i.e maximum similar words (default ${config.parameters.maxSimilarWords})")
  }

  override def run(spark: SparkSession, config: Config): Unit = {
    val sc = spark.sparkContext
    val wordFeatureCounts = sc.textFile(config.wordFeatureCountsCSV)
      .map(line => line.split("\t"))
      .map {
        case Array(word, feature, count) => (word, (feature, count.toInt))
        case _ => ("?", ("?", 0))
      }

    val wordCounts = sc.textFile(config.wordCountsCSV)
      .map(line => line.split("\t"))
      .map {
        case Array(word, count) => (word, count.toInt)
        case _ => ("?", 0)
      }

    val featureCounts = sc.textFile(config.featureCountsCSV)
      .map(line => line.split("\t"))
      .map {
        case Array(feature, count) => (feature, count.toInt)
        case _ => ("?", 0)
      }

    val (simsPath, simsWithFeaturesPath, featuresPath) =
      WordSimLib.computeWordSimsWithFeatures(
        wordFeatureCounts,
        wordCounts,
        featureCounts,
        config.outputDir,
        config.parameters)

    println(s"Word similarities: $simsPath")
    println(s"Word similarities with features: $simsWithFeaturesPath")
    println(s"Features: $featuresPath")
  }
}

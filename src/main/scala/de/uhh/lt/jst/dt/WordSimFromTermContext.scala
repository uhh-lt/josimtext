package de.uhh.lt.jst.dt

import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.dt.WordSimLib.{TermCountRDD, TermTermCountRDD, WordSimParameters}
import org.apache.spark.{SparkConf, SparkContext}

object WordSimFromTermContext extends SparkJob {

  case class Config(
    input: String = "",
    output: String = "",
    parameters: WordSimParameters = WordSimParameters()
  )

  type ConfigType = Config
  override val config = Config()

  override val command: String = "WordSimFromTermContext"
  override val description = "Compute a Distributional Thesaurus from a Term Context file."

  override val parser = new Parser {

    arg[String]("TERM_CONTEXT_FILE").action( (x, c) =>
      c.copy(input = x) ).required().hidden()

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(output = x) ).required().hidden()

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

  def calculateCountRDDsFromTextContext(sc: SparkContext, path: String):
    (TermTermCountRDD, TermCountRDD, TermCountRDD) = {

    val termContextRDD = sc.textFile(path)
      .map(line => line.split("\t"))
      .map{ case Array(term, context) => (term, context)}


    val termFeatureCountRDD = termContextRDD // Note: feature = context
      .map { case (term, context) => ((term, context), 1) }
      .reduceByKey(_ + _)
      .map { case ((term, context), count) => (term, (context, count))}

    val termCountRDD = termContextRDD
      .map { case (term, context) => (term, 1) }
      .reduceByKey(_ + _)

    val featureCountRDD = termContextRDD
      .map { case (term, context) => (context, 1) }
      .reduceByKey(_ + _)

    (termFeatureCountRDD, termCountRDD, featureCountRDD)
  }

  def run(sc: SparkContext, config: Config): Unit = {

    val (wordFeatureCounts, wordCounts, featureCounts) = calculateCountRDDsFromTextContext(sc, config.input)

    val (simsPath, simsWithFeaturesPath, featuresPath) =
      WordSimLib.computeWordSimsWithFeatures(
        wordFeatureCounts,
        wordCounts,
        featureCounts,
        config.output,
        config.parameters
      )

    println(s"Word similarities: $simsPath")
    println(s"Word similarities with features: $simsWithFeaturesPath")
    println(s"Features: $featuresPath")
  }
}

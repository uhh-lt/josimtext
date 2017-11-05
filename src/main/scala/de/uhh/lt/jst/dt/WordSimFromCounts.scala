package de.uhh.lt.jst.dt

import de.uhh.lt.jst.Job
import org.apache.spark.{SparkConf, SparkContext}
import scopt.{OptionDef, Read}


object WordSimFromCounts extends Job {

  case class Config(
    wordCountsCSV: String = "",
    featureCountsCSV: String = "",
    wordFeatureCountsCSV: String = "",
    outputDir: String = "",
    parameters: Parameters = Parameters()
  )

  case class Parameters (
    wordsPersFeature: Int = 1000,
    minSignificance: Double = 0.0,
    minWordFeatureCount: Int = 2,
    minWordCount: Int = 5,
    minFeatureCount: Int = 5,
    significanceType: String = "LMI",
    featuresPerWord: Int = 2000,
    maxSimilarWords: Int = 200
  )

  type ConfigType = Config
  override val config = Config()

  override val command: String = "WordSimFromCounts"
  override val description = "[DEPRECATED] Compute a Distributional Thesaurus from count files"

  override val parser = new Parser {

    arg[String]("WORD_COUNT_FILE").action( (x, c) =>
      c.copy(wordCountsCSV = x) ).required().hidden()

    arg[String]("FEATURE_COUNT_FILE").action( (x, c) =>
      c.copy(featureCountsCSV = x) ).required().hidden()

    arg[String]("WORD_FEATURE_COUNT_FILE").action( (x, c) =>
      c.copy(wordFeatureCountsCSV = x) ).required().hidden()

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDir = x) ).required().hidden()

    /*
    import scopt.Read
    import scopt.Read.reads
    implicit val paramsRead: Read[Parameters] = reads { str =>
      val paramsMap = implicitly[Read[Map[String, String]]].reads(str)
      try {
        Parameters() // FIXME complete me
      }
    }
    opt[Parameters]("params").action( (x, c) => c.copy(parameters = x) ) // FIXME EITHER compelete implicit and delete below OR remove it
  */

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

  def run(config: Config): Unit = oldMain(
    config.productIterator.flatMap {
      case product: Product => product.productIterator.map(_.toString).toList
      case x => List(x.toString)
    }.toArray
  )

  /*
    Array(
      config.wordCountsCSV,
      config.featureCountsCSV,
      config.wordFeatureCountsCSV,
      config.outputDir,
      config.parameters.wordsPerFeatureNum.toString,
      config.parameters.featuresPerWordNum.toString,
      config.parameters.wordMinCount.toString,
      config.parameters.featureMinCount.toString,
      config.parameters.wordFeatureMinCount.toString,
      config.parameters.significanceMin.toString,
      config.parameters.significanceType,
      config.parameters.similarWordsMaxNum.toString
    )
  */

  // ------ unchanged old logic ------- //

  val wordsPerFeatureNumDefault = 1000
  val significanceMinDefault = 0.0
  val wordFeatureMinCountDefault = 2
  val wordMinCountDefault = 2
  val featureMinCountDefault = 2
  val significanceTypeDefault = "LMI"
  val featuresPerWordNumDefault = 1000
  val similarWordsMaxNumDefault = 200

  def oldMain(args: Array[String]) {
    if (args.size < 4) {
      println("Usage: word-counts feature-counts word-feature-counts output-dir [parameters]")
      println("parameters: wordsPerFeatureNum featuresPerWordNum wordMinCount featureMinCount wordFeatureMinCount significanceMin significanceType similarWordsMaxNum")
      return
    }

    val wordCountsPath = args(0)
    val featureCountsPath = args(1)
    val wordFeatureCountsPath = args(2)
    val outputDir = args(3)
    val wordsPerFeatureNum = if (args.size > 4) args(4).toInt else wordsPerFeatureNumDefault
    val featuresPerWordNum = if (args.size > 5) args(5).toInt else featuresPerWordNumDefault
    val wordMinCount = if (args.size > 6) args(6).toInt else wordMinCountDefault
    val featureMinCount = if (args.size > 7) args(7).toInt else featureMinCountDefault
    val wordFeatureMinCount = if (args.size > 8) args(8).toInt else wordFeatureMinCountDefault
    val significanceMin = if (args.size > 9) args(9).toDouble else significanceMinDefault
    val significanceType = if (args.size > 10) args(10) else significanceTypeDefault
    val similarWordsMaxNum = if (args.size > 11) args(11).toInt else similarWordsMaxNumDefault

    val conf = new SparkConf().setAppName("JST: WordSimFromCounts")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    run(sc, wordCountsPath, featureCountsPath, wordFeatureCountsPath, outputDir, wordsPerFeatureNum, featuresPerWordNum, wordMinCount, featureMinCount, wordFeatureMinCount, significanceMin, significanceType, similarWordsMaxNum)
  }

  def run(sc: SparkContext,
          wordCountsPath: String,
          featureCountsPath: String,
          wordFeatureCountsPath: String,
          outputDir: String,
          wordsPerFeatureNum: Int = wordsPerFeatureNumDefault,
          featuresPerWordNum: Int = featuresPerWordNumDefault,
          wordMinCount: Int = wordMinCountDefault,
          featureMinCount: Int = featureMinCountDefault,
          wordFeatureMinCount: Int = wordFeatureMinCountDefault,
          significanceMin: Double = significanceMinDefault,
          significanceType: String = significanceTypeDefault,
          similarWordsMaxNum: Int = similarWordsMaxNumDefault) = {

    val wordFeatureCounts = sc.textFile(wordFeatureCountsPath)
      .map(line => line.split("\t"))
      .map {
        case Array(word, feature, count) => (word, (feature, count.toInt))
        case _ => ("?", ("?", 0))
      }

    val wordCounts = sc.textFile(wordCountsPath)
      .map(line => line.split("\t"))
      .map {
        case Array(word, count) => (word, count.toInt)
        case _ => ("?", 0)
      }

    val featureCounts = sc.textFile(featureCountsPath)
      .map(line => line.split("\t"))
      .map {
        case Array(feature, count) => (feature, count.toInt)
        case _ => ("?", 0)
      }

    val (simsPath, simsWithFeaturesPath, featuresPath) = WordSimLib.computeWordSimsWithFeatures(wordFeatureCounts, wordCounts, featureCounts, outputDir, wordsPerFeatureNum, featuresPerWordNum, wordMinCount, featureMinCount, wordFeatureMinCount, significanceMin, similarWordsMaxNum, significanceType)

    println(s"Word similarities: $simsPath")
    println(s"Word similarities with features: $simsWithFeaturesPath")
    println(s"Features: $featuresPath")
  }
}

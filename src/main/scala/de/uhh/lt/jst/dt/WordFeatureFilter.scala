package de.uhh.lt.jst.dt

import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.utils.Util
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object WordFeatureFilter  extends SparkJob {

  case class Config(
    wordFeatureCSV: String = "",
    vocCSV: String = "",
    outputWordFeatureCSV: String = ""
  )

  type ConfigType = Config
  override val config = Config()

  override val command: String = "WordFeatureFilter"
  override val description = "Remove all rows where the word is not in the VOC_FILE"

  override val parser = new Parser {
    arg[String]("WORD_FEATURE_COUNT_FILE").action((x, c) =>
      c.copy(wordFeatureCSV = x)).required().hidden()

    arg[String]("VOC_FILE").action((x, c) =>
      c.copy(vocCSV = x)).required().hidden()

    arg[String]("OUTPUT_DIR").action((x, c) =>
      c.copy(outputWordFeatureCSV = x)).required().hidden()
  }

  def run(spark: SparkSession, config: Config): Unit = {
    val sc = spark.sparkContext
    // Filter
    val voc = Util.loadVocabulary(sc, config.vocCSV)
    val (wordFeatureFreq, featureVoc) = calculate(config.wordFeatureCSV, voc, sc)

    val outFeatureVocPath = config.outputWordFeatureCSV + "-voc.csv"
    // Save result
    featureVoc
      .saveAsTextFile(outFeatureVocPath)

    wordFeatureFreq
      .map({ case (word, feature, freq) => word + "\t" + feature + "\t" + freq })
      .saveAsTextFile(config.outputWordFeatureCSV)
  }

  def calculate(inputPath: String, voc: Set[String], sc: SparkContext): (RDD[(String, String, String)], RDD[String]) = {
    // Filter

    val wordFeatureFreq = sc.textFile(inputPath)
      .map(line => line.split("\t"))
      .map({ case Array(word, feature, freq) => (word, feature, freq) case _ => ("?", "?", "?") })
      .filter({ case (word, feature, freq) => (voc.contains(word.toLowerCase())) })
      .cache()

    val features = wordFeatureFreq
      .map({ case (word, feature, freq) => (feature) })
      .distinct()
      .sortBy({ case feature => feature })
      .cache()
    (wordFeatureFreq, features)
  }
}
package de.uhh.lt.jst.dt

import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.utils.Util
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object FreqFilter extends SparkJob {

  case class Config(
    freqCSV: String = "",
    vocabularyCSV: String = "",
    outputFreqCSV: String = "",
    keepSingleWords: Boolean = true
  )

  type ConfigType = Config
  override val config = Config()

  override val command: String = "FreqFilter"
  override val description = "Remove all rows where the term is not in the VOC_FILE"

  override val parser = new Parser {

    opt[Unit]('s', "remove-single").action( (x, c) =>
      c.copy(keepSingleWords = false) ).
      text("remove all single word terms")

    arg[String]("TERM_REPR_FILE").action( (x, c) =>
      c.copy(freqCSV = x) ).required().hidden()

    arg[String]("VOC_FILE").action( (x, c) =>
      c.copy(vocabularyCSV = x) ).required().hidden()

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputFreqCSV = x) ).required().hidden()
  }

  override def run(spark: SparkSession, config: Config): Unit = {
    val sc = spark.sparkContext
    // Filter
    val voc = Util.loadVocabulary(sc, config.vocabularyCSV)
    val freqFiltered = calculate(sc, config.freqCSV, voc, config.keepSingleWords)

    // Save the result
    freqFiltered
      .map({ case (word, freq) => word + "\t" + freq })
      .saveAsTextFile(config.outputFreqCSV)
  }

  def calculate(sc: SparkContext, freqPath: String, voc: Set[String], keepSingleWords: Boolean): RDD[(String, String)] = {
    val freq = sc.textFile(freqPath)
      .map(line => line.split("\t"))
      .map({ case Array(word, freq) => (word, freq) case _ => ("?", "?") })

    val freqFiltered =
      if (keepSingleWords) {
        freq.filter({ case (word, freq) => (!word.contains(" ") || voc.contains(word.toLowerCase())) })
      } else {
        freq.filter({ case (word, freq) => (voc.contains(word.toLowerCase())) })
      }
    freqFiltered
  }
}
package de.uhh.lt.jst.corpus


import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.utils.{Const, Util}
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.util.Try

object CoarsifyPosTags extends SparkJob {

  case class Config(
    inputDir: String = "",
    outputDir: String = ""
  )

  override type ConfigType = Config
  override val config = Config()

  override val description: String = ""
  override val parser = new Parser {

    arg[String]("INPUT_DIR").action( (x, c) =>
      c.copy(inputDir = x) ).required().
      text("Directory with word-feature counts (i.e. W-* files, F-* files, WF-* files).'")

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDir = x) ).required().
      text("Directory with output word-feature counts where W-* and WF-* files " +
        "contain coarsified POS tags (44 Penn POS tags --> 21 tags).")
  }

  val posLookup = Try(
    Source
      .fromURL(getClass.getResource("/pos-tags.csv"))
      .getLines
      .map {
        _.split("\t")
      }
      .map { case Array(freq, posOrig, posNew) => (posOrig, posNew) }
      .toMap)
    .getOrElse(Map[String, String]())

  def full2coarse(fullPos: String) = {
    if (posLookup.contains(fullPos)) {
      posLookup(fullPos)
    } else {
      fullPos
    }
  }


  /**
    * Takeas as input a pos-tagged word like "Python#NNP" and outputs
    * another entry like "Python#NN". Each tag of MWE, e.g. "Python#NNP language#NN"
    * is transformed accordingly to "Python#NNP language#NN" */
  def coarsifyPosTag(lexitemWithPosTag: String) = {
    lexitemWithPosTag
      .split(" ")
      .map { case wordWithPosTag => coarsifyWordPosTag(wordWithPosTag) }
      .mkString(" ")
  }

  def coarsifyWordPosTag(wordWithPosTag: String) = {
    val fields = wordWithPosTag.split(Const.POS_SEP)
    if (fields.length < 2) {
      wordWithPosTag
    } else {
      val head = fields
        .slice(0, fields.length - 1)
        .mkString(Const.POS_SEP)

      val tail = posLookup.getOrElse(fields.last, fields.last)

      head + Const.POS_SEP + tail
    }
  }

  def run(spark: SparkSession, config: Config) = {
    val outputWordCountsPath = config.outputDir + "/W"
    val sc = spark.sparkContext
    Util.delete(outputWordCountsPath)
    sc.textFile(config.inputDir + "/W-*")
      .map { line => line.split("\t") }
      .map { case Array(word, freq) => (word, freq.toLong) case _ => ("?", -1.toLong) }
      .map { case (word, freq) => (coarsifyPosTag(word), freq) }
      .reduceByKey {
        _ + _
      }
      .map { case (word, freq) => (freq, word) }
      .sortByKey(ascending = false)
      .map { case (freq, word) => s"$word\t$freq" }
      .saveAsTextFile(outputWordCountsPath)

    val outputWordFeatureCountsPath = config.outputDir + "/WF"
    Util.delete(outputWordFeatureCountsPath)
    sc.textFile(config.inputDir + "/WF-*")
      .map { line => line.split("\t") }
      .map { case Array(word, feature, freq) => (word, feature, freq.toLong) case _ => ("?", "?", -1.toLong) }
      .map { case (word, feature, freq) => ((coarsifyPosTag(word), feature), freq) }
      .reduceByKey {
        _ + _
      }
      .map { case ((word, feature), freq) => s"$word\t$feature\t$freq" }
      .saveAsTextFile(outputWordFeatureCountsPath)

    val outputFeaturesCountsPath = config.outputDir + "/F"
    Util.delete(outputFeaturesCountsPath)
    sc.textFile(config.inputDir + "/F-*")
      .saveAsTextFile(outputFeaturesCountsPath) // just copy to new location
  }

}

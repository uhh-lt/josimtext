package de.uhh.lt.jst.dt

import de.uhh.lt.jst.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DTFilter extends SparkJob {

  case class Config(
    dt: String = "",
    vocabulary: String = "",
    outputDTDirectory: String = "",
    keepSingleWords: Boolean = true,
    filterOnlyTarget: Boolean = false
  )

  type ConfigType = Config
  override val config = Config()

  override val command: String = "DTFilter"
  override val description = "Remove all target and related words which are not in the VOC_FILE."

  val parser = new Parser {

    opt[Unit]('s', "remove-single").action( (x, c) =>
      c.copy(keepSingleWords = false) ).
      text("remove all rows with single words")

    opt[Unit]('t', "only-target").action( (x, c) =>
      c.copy(filterOnlyTarget = true) ).
      text("only remove target words not related words")

    arg[String]("DT_FILE").action( (x, c) =>
      c.copy(dt = x) ).required().hidden()

    arg[String]("VOC_FILE").action( (x, c) =>
      c.copy(vocabulary = x) ).required().hidden()

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDTDirectory = x) ).required().hidden()
  }

  override def run(spark: SparkSession, config: Config) = {
    val sc = spark.sparkContext
    val voc = sc.textFile(config.vocabulary)
      .map(line => line.split("\t"))
      .map { case Array(word) => word.trim().toLowerCase() }
      .collect()
      .toSet
    println(s"Vocabulary size: ${voc.size}")

    val dt = sc.textFile(config.dt)
      .map(line => line.split("\t"))
      .map {
        case Array(word_i, word_j, sim_ij, features_ij) => (word_i, word_j, sim_ij, features_ij)
        case Array(word_i, word_j, sim_ij) => (word_i, word_j, sim_ij, "")
        case _ => ("?", "?", "?", "?")
      }

    val dt_filter =
      if (config.keepSingleWords && config.filterOnlyTarget) {
        dt.filter { case (word_i, word_j, sim_ij, features_ij) => voc.contains(word_i.toLowerCase()) || !word_i.contains(" ") }
      } else if (!config.keepSingleWords && config.filterOnlyTarget) {
        dt.filter { case (word_i, word_j, sim_ij, features_ij) => voc.contains(word_i.toLowerCase()) }
      } else if (config.keepSingleWords && !config.filterOnlyTarget) {
        dt.filter { case (word_i, word_j, sim_ij, features_ij) => (!word_i.contains(" ") || voc.contains(word_i.toLowerCase()) && (!word_j.contains(" ") || voc.contains(word_j.toLowerCase()))) }
      } else { // if (!keepSingleWords && !filterOnlyTarget){
        dt.filter { case (word_i, word_j, sim_ij, features_ij) => (voc.contains(word_i.toLowerCase()) && (voc.contains(word_j.toLowerCase()))) }
      }

    dt_filter
      .map { case (word_i, word_j, sim_ij, features_ij) => word_i + "\t" + word_j + "\t" + sim_ij }
      .saveAsTextFile(config.outputDTDirectory)
  }

}
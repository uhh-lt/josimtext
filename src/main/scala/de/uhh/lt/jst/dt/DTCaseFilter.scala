package de.uhh.lt.jst.dt

import de.uhh.lt.jst.{SparkJob, utils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

object DTCaseFilter extends SparkJob {

  case class Config(dt: String = "", outputDTDirectory: String = "")

  type ConfigType = Config
  override val config = Config()

  override val command: String = "DTCaseFilter"
  override val description = "Filter out target words " +
    "that at not all small caps or first capital + all small caps"

  override val parser = new Parser {
    arg[String]("DT_FILE").action( (x, c) =>
      c.copy(dt = x) ).required().hidden()

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDTDirectory = x) ).required().hidden()
  }


  def run(spark: SparkSession, config: Config): Unit = {

    val sc = spark.sparkContext
    val dt = sc.textFile(config.dt)
      .map(line => line.split("\t"))
      .map {
        case Array(word_i, word_j, sim_ij, features_ij) => (word_i, word_j, sim_ij, features_ij)
        case Array(word_i, word_j, sim_ij) => (word_i, word_j, sim_ij, "?")
        case _ => ("?", "?", "?", "?")
      }

    dt.filter {
      case (word_i, word_j, sim_ij, features_ij) =>
        (word_i.length <= 1) || (word_i.substring(1).toLowerCase() == word_i.substring(1))
    }.map {
      case (word_i, word_j, sim_ij, features_ij) =>
        word_i + "\t" + word_j + "\t" + sim_ij
    }.saveAsTextFile(config.outputDTDirectory)
  }
}
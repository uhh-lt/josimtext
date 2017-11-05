package de.uhh.lt.jst.dt

import de.uhh.lt.jst.{Job, utils}
import org.apache.spark.{SparkConf, SparkContext}

object DTCaseFilter extends Job {

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

  def run(config: Config): Unit = {

    val conf = new SparkConf().setAppName("DTFilter")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    run(sc, config.dt, config.outputDTDirectory)
  }

  def run(sc: SparkContext, dtPath: String, outPath: String): Unit = {
    println("Input DT: " + dtPath)
    println("Output DT: " + outPath)
    utils.Util.delete(outPath)

    val dt = sc.textFile(dtPath)
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
    }.saveAsTextFile(outPath)
  }
}
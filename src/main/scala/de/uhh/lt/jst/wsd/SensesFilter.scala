package de.uhh.lt.jst.wsd

import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.utils.{Const, Util}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SensesFilter extends SparkJob {

  case class Config(
    inputDir: String = "",
    vocFile: String = "",
    outputDir: String = "",
    lowerCase: Boolean = false,
    lowerOrFirstUpper: Boolean = false
  )

  override type ConfigType = Config
  override val config = Config()
  override val description = ""
  override val parser = new Parser {

    arg[String]("INPUT_DIR").action( (x, c) =>
      c.copy(inputDir = x) ).required().
      text("A CSV file with sense clusters in the 'JS' format 'target<TAB>sense<TAB>cluster', cluster being 'word:sim'<SPACE><SPACE>'")

    arg[String]("VOC_FILE").action( (x, c) =>
      c.copy(vocFile = x) ).required().text("A list of target words that will be kept in the output sense clusters")

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDir = x) ).required().text("Output senses in the same format as input"
    )

    opt[Unit]('i', "ignoreCase").action( (x, c) =>
      c.copy(lowerCase = true) ).
      text("Ignore case in vocabulary and sense inventory.")

    opt[Unit]('o', "onlyIgnoreFirstLetterCase").action( (x, c) =>
      c.copy(lowerOrFirstUpper = true) ).
      text("Only 'apple' or 'Apple' are kept, but not 'APPLE' or 'AppLe'.")
  }

  def run(spark: SparkSession, config: Config): Unit = {
    val sc = spark.sparkContext
    val outVocPath = config.vocFile + "-voc.csv"

    val voc = Util.loadVocabulary(sc, config.vocFile)
    val (senses, clusterVoc) = run(config.inputDir, voc, sc, config.lowerCase)

    senses
      .map({ case (target, sense_id, cluster) => target + "\t" + sense_id + "\t" + cluster })
      .saveAsTextFile(config.outputDir)

    clusterVoc
      .saveAsTextFile(outVocPath)
  }

  def run(inSensesPath: String, voc: Set[String], sc: SparkContext, lowercase: Boolean = true, lowerOrFirstUpper: Boolean = true): (RDD[(String, String, String)], RDD[String]) = {
    val senses: RDD[(String, String, String)] = sc.textFile(inSensesPath)
      .map { line => line.split("\t") }
      .map {
        case Array(target, sense_id, cluster) => (target, sense_id, cluster)
        case Array(target, sense_id, cluster, isas) => (target, sense_id, cluster)
        case _ => ("?", "0", "")
      }
      .filter { case (target, sense_id, cluster) => voc.contains(if (lowercase) target.toLowerCase() else target) }
      .filter { case (target, sense_id, cluster) => !lowerOrFirstUpper || target.length <= 1 || target.substring(1).toLowerCase() == target.substring(1) }
    val clusterVoc = senses
      .map { case (target, sense_id, cluster) => (cluster) }
      .flatMap { case (cluster) => cluster.split(Const.LIST_SEP) }
      .map { case cluster => cluster.split(":")(0) }
      .distinct()
      .sortBy { case cluster => cluster }

    (senses, clusterVoc)
  }
}
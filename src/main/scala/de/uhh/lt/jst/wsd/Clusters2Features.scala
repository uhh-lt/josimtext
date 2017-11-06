package de.uhh.lt.jst.wsd

import de.uhh.lt.jst.SparkJob
import org.apache.spark.sql.SparkSession


object Clusters2Features  extends SparkJob {
  case class Config(
    inputDir: String = "",
    outputDir: String = ""
  )
  override type ConfigType = Config
  override val config = Config()
  override val description = "Converts sense inventorie file formats"

  override val parser = new Parser {
    note("Makes a WSD features file 'word<TAB>sense-id<TAB>cluster<TAB>features' from the old format sense" +
      "inventory file 'word<TAB>sense-id<TAB>keyword<TAB>cluster', where 'features' are the same as the cluster.")
    arg[String]("INPUT_DIR").action((x, c) =>
      c.copy(inputDir = x)).required().hidden()

    arg[String]("OUTPUT_DIR").action((x, c) =>
      c.copy(outputDir = x)).required().hidden()
  }

  def run(spark: SparkSession, config: Config): Unit = {
    spark.sparkContext
      .textFile(config.inputDir)
      .map(line => line.split("\t"))
      .map { case Array(target, sense_id, keyword, cluster) => (target, sense_id, cluster) case _ => ("?", "-1", "") }
      .filter { case (target, sense_id, cluster) => target != "?" }
      .map { case (target, sense_id, cluster) => f"$target\t$sense_id\t$cluster\t$cluster" }
      .saveAsTextFile(config.outputDir)
  }
}

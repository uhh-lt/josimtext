package de.uhh.lt.jst.corpus

import de.uhh.lt.jst.SparkJob
import org.apache.spark.SparkContext

object ReformatConll extends SparkJob {

  case class Config(
    inputDir: String = "",
    outputDir: String = ""
  )

  override type ConfigType = Config
  override val config = Config()

  val oldConllRecordDelimiter = "^-1\t".r
  val newConllRecordDelimiter = ">>>>>\t"

  override val description: String = ""
  override val parser = new Parser {

    arg[String]("INPUT_DIR").action( (x, c) =>
      c.copy(inputDir = x) ).required().
      text(s"Directory with a parsed corpus in the CoNLL format: delimiter='$oldConllRecordDelimiter'.")

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDir = x) ).required().
      text("Directory with a parsed corpus in the CoNLL format: delimiter='$newConllRecordDelimiter'.")
  }

  override def run(sc: SparkContext, config: Config): Unit = {
    sc
      .textFile(config.inputDir)
      .map { line => oldConllRecordDelimiter.replaceAllIn(line, newConllRecordDelimiter) }
      .saveAsTextFile(config.outputDir)
  }
}

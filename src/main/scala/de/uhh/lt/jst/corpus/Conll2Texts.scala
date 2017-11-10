package de.uhh.lt.jst.corpus

import de.uhh.lt.jst.SparkJob
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Conll2Texts extends SparkJob {

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
      text("Directory with a parsed corpus in the CoNLL format.")

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDir = x) ).required().
      text("Directory with a corpus text format derived from CoNLL " +
        "(but without any linguistic annotation)")
  }

  val textRegex = """# text = (.*)""".r
  val newdocRegex = """# newdoc""".r

  def getText(line:String): String = {
    val textMatch = textRegex.findFirstMatchIn(line)
    if (textMatch.isDefined) textMatch.get.group(1).trim
    else line
  }

  def addDocumentBreaks(line:String): String = {
    val textMatch = newdocRegex.findFirstMatchIn(line)
    if (textMatch.isDefined) "\n\n" + line
    else line
  }

  override def run(spark: SparkSession, config: Config): Unit = {
    spark.sparkContext
      .textFile(config.inputDir)
      .filter { line => line.startsWith("# ")}
      .filter{ line => !line.startsWith("# parser") && !line.startsWith("# sent_id")}
      .map{ line => getText(line)}
      .map{ line => addDocumentBreaks(line)}
      .saveAsTextFile(config.outputDir, classOf[GzipCodec])
  }
}

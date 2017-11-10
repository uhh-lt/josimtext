package de.uhh.lt.jst.corpus

import de.uhh.lt.jst.SparkJob
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object StoreToElasticSearch extends SparkJob {
  case class Config(inputDir: String = "", outputIndex: String = "")
  override type ConfigType = Config
  override val config = Config()
  override val description: String = "Index CoNLL file with ElasticSearch"

  override val parser = new Parser {
    arg[String]("INPUT_DIR").action( (x, c) =>
      c.copy(inputDir = x) ).required().
      text("Directory with a parsed corpus in the CoNLL format.")

    arg[String]("OUTPUT_INDEX").action( (x, c) =>
      c.copy(outputIndex = x) ).required().
      text("Name of the output ElasticSearch index that will be created.")
  }

  val textRegex = """# text = (.*)""".r
  val newdocRegex = """# newdoc""".r
  val indexName = "spark/index"

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

  var i = 1
  def getDocumentMap(line: String): Map[String, String] = {
    i = i+1
    val entry = Map("index" ->i.toString, "text" ->line)
    entry
  }

  override def run(spark: SparkSession, config: Config): Unit = {
    spark.conf.set("es.index.auto.create", "true")

    spark.sparkContext
      .textFile(config.inputDir)
      .filter { line => line.startsWith("# ")}
      .filter{ line => !line.startsWith("# parser") && !line.startsWith("# sent_id")}
      .map{ line => getText(line)}
      .map{ line => addDocumentBreaks(line)}
      .map{ line => getDocumentMap(line)}
      .saveToEs(indexName)

  }
}

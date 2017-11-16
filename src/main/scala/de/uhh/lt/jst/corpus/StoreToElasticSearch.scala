package de.uhh.lt.jst.corpus

import de.uhh.lt.jst.Job
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

object StoreToElasticSearch extends Job {
  case class Config(inputDir: String = "",
                    outputIndex: String = "depcc/sentences",
                    esNodeList: String = "ltheadnode")
  override type ConfigType = Config
  override val config = Config()
  override val description: String = "Index CoNLL file with ElasticSearch"

  override val parser = new Parser {
    arg[String]("INPUT_DIR").action( (x, c) =>
      c.copy(inputDir = x) ).required().
      text("Directory with a parsed corpus in the CoNLL format.")

    arg[String]("OUTPUT_INDEX").action( (x, c) =>
      c.copy(outputIndex = x) ).required().
      text("Name of the output ElasticSearch index that will be created in the 'index/type' format.")

    arg[String]("ES_NODES").action( (x, c) =>
      c.copy(esNodeList = x) ).required().
      text("List of ElasticSearch nodes where the output will be written (may be not exhaustive).")
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

  def run(spark: SparkSession, config: ConfigType): Unit = {
    spark.sparkContext
      .textFile(config.inputDir)
      .filter { line => line.startsWith("# ")}
      .filter{ line => !line.startsWith("# parser") && !line.startsWith("# sent_id")}
      .map{ line => getText(line)}
      .map{ line => addDocumentBreaks(line)}
      .map{ line => Map("sentence" ->line)}
      .saveToEs(config.outputIndex)
  }

  override def run(config: ConfigType): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("es.index.auto.create", "true")
      .config("es.nodes", config.esNodeList)
      //.config (read login and password of ES cluster here
      .getOrCreate()

    run(spark, config)
  }
}

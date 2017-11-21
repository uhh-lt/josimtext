package de.uhh.lt.jst.corpus

import de.uhh.lt.conll.Sentence
import de.uhh.lt.jst.Job
import de.uhh.lt.jst.verbs.Conll2Features.{Dependency, conllRecordDelimiter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import de.uhh.lt.conll._
import scala.util.Try
import de.uhh.lt.jst.utils.Util

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


  def run(spark: SparkSession, config: ConfigType): Unit = {

    val conf = new Configuration
    conf.set("textinputformat.record.delimiter", "\n\n")

    spark.sparkContext
      .newAPIHadoopFile(config.inputDir, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map { record => record._2.toString }
      .map { CoNLLParser.parseSingleSentence }
      .map{ sentence => Map(
        "document_id" -> sentence.documentID,
        "sentence_id" -> sentence.sentenceID,
        "text" -> sentence.text,
        "deps" -> sentence.deps
            .map{d => s"${d._2.lemma}--${d._2.deprel}--${sentence.deps(d._2.head).lemma}"})
      }
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

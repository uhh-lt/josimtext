package de.uhh.lt.jst.index

import de.uhh.lt.conll._
import de.uhh.lt.jst.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._


abstract class ElasticSearchIndexer extends Job {
  case class Config(insertID: String = "",
                    inputDir: String = "",
                    outputIndex: String = "depcc/sentences",
                    esNodeList: String = "localhost",
                    maxBatchMb: Int = 1,
                    maxBatchDocs: Int = 1000,
                    user: String = "",
                    password: String = "")
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

    arg[Int]("MAX_BATCH_MB").action( (x, c) =>
      c.copy(maxBatchMb = x * 1000000) ).
      text("Max. size of a batch in MB.")

    arg[Int]("MAX_BATCH_DOCS").action( (x, c) =>
      c.copy(maxBatchDocs = x) ).
      text("Max. size of a batch in number of documents.")

    arg[String]("INSERT_ID").action( (x, c) =>
      c.copy(insertID = x) ).required().
      text("Identifier of the insert batch of documents.")

    arg[String]("USER").action( (x, c) =>
      c.copy(user = x) ).required().
      text("ES basic http auth user name.")

    arg[String]("PASS").action( (x, c) =>
      c.copy(password = x) ).required().
      text("ES basic http auth password.")
  }

  abstract def run(spark: SparkSession, config: ConfigType): Unit

  override def run(config: ConfigType): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("es.index.auto.create", "true")
      .config("es.nodes", config.esNodeList)
      .config("es.http.retries", "999")
      .config("es.batch.write.retry.count", "999")
      .config("es.batch.write.retry.wait", "300")
      .config("es.batch.size.bytes", config.maxBatchMb.toString)
      .config("es.batch.size.entries", config.maxBatchDocs.toString)
      .config("es.net.http.auth.user", config.user)
      .config("es.net.http.auth.pass", config.password)
      .getOrCreate()

    run(spark, config)
  }
}

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
                    password: String = "",
                    retries: Int = 9999,
                    writeTimeoutSec: Int = 30)
  override type ConfigType = Config
  override val config = Config()

  override val parser = new Parser {
    arg[String]("INPUT_DIR").action( (x, c) =>
      c.copy(inputDir = x) ).required().
      text(s"Directory with a parsed corpus in the CoNLL format.")

    arg[String]("OUTPUT_INDEX").action( (x, c) =>
      c.copy(outputIndex = x) ).required().
      text("Name of the output ElasticSearch index that will be created in the 'index/type' format.")

    opt[String]("es-nodes").action( (x, c) =>
      c.copy(esNodeList = x) ).
      text(s"List of some ElasticSearch nodes of a cluster (default: ${config.esNodeList}).")

    opt[Int]("max-batch-mb").action( (x, c) =>
      c.copy(maxBatchMb = x * 1000000) ).
      text(s"Max. size of a batch in MB (default: ${config.maxBatchMb}).")

    opt[Int]("max-batch-doc").action( (x, c) =>
      c.copy(maxBatchDocs = x) ).
      text(s"Max. size of a batch in number of documents (default: ${config.maxBatchDocs}).")

    opt[String]("insert-id").action( (x, c) =>
      c.copy(insertID = x) ).
      text(s"Identifier of the insert batch of documents (default: ${config.insertID}).")

    opt[String]("user").action( (x, c) =>
      c.copy(user = x) ).
      text(s"ES basic http auth user name (default: ${config.user}).")

    opt[String]("pass").action( (x, c) =>
      c.copy(password = x) ).
      text(s"ES basic http auth password (default: ${config.password}).")

    opt[String]("retries-num").action( (x, c) =>
      c.copy(password = x) ).
      text(s"Maximum number of retries (default: ${config.retries}).")

    opt[String]("write-timeout-sec").action( (x, c) =>
      c.copy(password = x) ).
      text(s"Maximum number of retries (default: ${config.writeTimeoutSec}).")
  }

  def run(spark: SparkSession, config: ConfigType): Unit

  override def run(config: ConfigType): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("es.index.auto.create", "true")
      .config("es.nodes", config.esNodeList)
      .config("es.batch.size.bytes", config.maxBatchMb.toString)
      .config("es.batch.size.entries", config.maxBatchDocs.toString)
      .config("es.net.http.auth.user", config.user)
      .config("es.net.http.auth.pass", config.password)
      .config("es.http.retries", config.retries)
      .config("es.batch.write.retry.count", config.retries)
      .config("es.batch.write.retry.wait", config.writeTimeoutSec)
      .getOrCreate()

    run(spark, config)
  }
}

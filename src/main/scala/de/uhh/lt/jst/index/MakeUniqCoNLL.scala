package de.uhh.lt.jst.index

import de.uhh.lt.conll._
import de.uhh.lt.jst.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._


class MakeUniqCoNLL extends Job {
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
      text("Directory with a parsed corpus in the CoNLL format (only unique sentences).")
  }

  def run(spark: SparkSession, config: ConfigType): Unit = {
    // read from text file
    // group by sentence id
    // take only one sentence per group
    // output to a text file
  }

  override def run(config: ConfigType): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    run(spark, config)
  }
}

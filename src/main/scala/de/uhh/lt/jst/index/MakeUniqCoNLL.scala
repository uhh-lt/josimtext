package de.uhh.lt.jst.index

import de.uhh.lt.conll._
import de.uhh.lt.jst.{Job, SparkJob}
import de.uhh.lt.jst.utils.Util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._


object MakeUniqCoNLL extends SparkJob {

  case class Config(inputDir: String = "",
                    outputDir: String = "")
  override type ConfigType = Config
  override val config = Config()
  override val description: String = "Index CoNLL file with ElasticSearch"

  override val parser = new Parser {
    arg[String]("INPUT_DIR").action( (x, c) =>
      c.copy(inputDir = x) ).required().
      text("Directory with a parsed corpus in the CoNLL format.")

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDir = x) ).required().
      text("Directory with a parsed corpus in the CoNLL format (only unique sentences).")
  }

  def run(spark: SparkSession, config: ConfigType): Unit = {
    println(s"Input: ${config.inputDir}")
    println(s"Output: ${config.outputDir}")

    // group by sentence id
    // take only one sentence per group

    Util.delete(config.outputDir)

    val hadoopConfig = new Configuration
    hadoopConfig.set("textinputformat.record.delimiter", "\n\n")

    spark.sparkContext
      .newAPIHadoopFile(config.inputDir, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConfig)
      .map{ record => record._2.toString }
      .map{ CoNLLParser.parseSingleSentence }
      .map{ sentence => (sentence.text.hashCode, (sentence, 1)) }
      .reduceByKey{ (s1, s2) => (s1._1, s1._2 + s2._2) }
      .map{ s => s"${s._1}\t${s._2._2}\t${s._2._1.text}" }
      //.map{ kg => s"${kg._1}\t${kg._2.head.text}\t${kg._2.size}" }
      .saveAsTextFile(config.outputDir)
  }
}

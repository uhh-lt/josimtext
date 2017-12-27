package de.uhh.lt.jst.index

import de.uhh.lt.conll.CoNLLParser
import de.uhh.lt.jst.SparkJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

object UniqIndexerCoNLL extends SparkJob {

  case class Config(insertID: String = "",
                    inputDir: String = "",
                    outputIndex: String = "depcc/sentences")

  override type ConfigType = Config
  override val config = Config()

  override val parser = new Parser {
    arg[String]("INPUT_DIR").action( (x, c) =>
      c.copy(inputDir = x) ).required().
      text("Directory with a parsed corpus in the CoNLL format.")

    arg[String]("OUTPUT_INDEX").action( (x, c) =>
      c.copy(outputIndex = x) ).required().
      text("Name of the output ElasticSearch index that will be created in the 'index/type' format.")

    opt[String]("insert-id").action( (x, c) =>
      c.copy(insertID = x) ).required().
      text("Identifier of the insert batch of documents.")
  }

  override val description: String = "Index unique sentences in the CoNLL file with ElasticSearch (no provenance information)."

  override def run(spark: SparkSession, config: ConfigType): Unit = {
    val hadoopConfig = new Configuration
    hadoopConfig.set("textinputformat.record.delimiter", "\n\n")

    spark.sparkContext
      .newAPIHadoopFile(config.inputDir, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConfig)
      .map { record => record._2.toString }
      .map { CoNLLParser.parseSingleSentence }
      .map{ sentence => Map(
        "insert_id" -> config.insertID, // to be able to insert in chunks and then roll back failed chunks
        "sentence_hash" -> sentence.hashCode,
        "text" -> sentence.text,
        // more linguistic annotations can be inserted here (named entities, ...)
        "deps" -> sentence.deps
          .map{d => (d._2.deprel, d._2.lemma, sentence.deps(d._2.head).lemma)}
        , "conll" -> sentence.deps.map(_._2).toList.sortBy(_.id).mkString("\n"))
      }
      .saveToEs(config.outputIndex)
  }

}


/* Mapping:

PUT depcc
{
}
*/
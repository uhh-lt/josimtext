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

object StoreToElasticSearch extends Job {
  case class Config(inputDir: String = "",
                    outputIndex: String = "depcc/sentences",
                    esNodeList: String = "localhost",
                    maxBatchMb: Int = 1,
                    maxBatchDocs: Int = 1000)
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
            .map{d => s"${d._2.deprel} >>> ${d._2.lemma} <<< ${sentence.deps(d._2.head).lemma}"}
        , "deps_raw" -> sentence.deps.map(_._2).toList.sortBy(_.id).mkString("\n"))
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
      .config("es.http.retries", "50")
      .config("es.batch.write.retry.count", "50")
      .config("es.batch.write.retry.wait", "300")
      .config("es.batch.size.bytes", config.maxBatchMb.toString)
      .config("es.batch.size.entries", config.maxBatchDocs.toString)
      //.config (read login and password of ES cluster here
      .getOrCreate()

    run(spark, config)
  }
}

/* Mapping:

PUT test_default
{
  "mappings": {
    "sentences": {
      "properties": {
        "document_id": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "text": {
          "type": "text",
          "analyzer": "english",
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          }
        },
        "sentence_id": {
          "type": "long"
        },
        "deps": {
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "type": "text"
        },
        "deps_raw": {
          "type": "text",
          "index": false
        }
      }
    }
  },
  "settings": {
    "analysis": {
      "analyzer": {
        "my_english_analyzer":{
          "type": "english"
        }
      }
    },
    "index": {
      "number_of_shards": "64",
      "number_of_replicas": "1"
    }
  }
}

 */

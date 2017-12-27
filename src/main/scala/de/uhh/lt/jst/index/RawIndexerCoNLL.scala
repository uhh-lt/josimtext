package de.uhh.lt.jst.index

import de.uhh.lt.conll.CoNLLParser
import de.uhh.lt.jst.ElasticSearchSparkJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._


object RawIndexerCoNLL extends ElasticSearchSparkJob {

  case class Config(insertID: String = "",
                    inputDir: String = "",
                    outputIndex: String = "depcc/sentences")

  override type ConfigType = Config
  override val config = Config()

  override val parser = new ElasticSearchParser {
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

  override val description: String = "Index raw CoNLL file with ElasticSearch"

  override def run(spark: SparkSession, config: ConfigType): Unit = {
    val hadoopConfig = new Configuration
    hadoopConfig.set("textinputformat.record.delimiter", "\n\n")

    spark.sparkContext
      .newAPIHadoopFile(config.inputDir, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConfig)
      .map { record => record._2.toString }
      .map { CoNLLParser.parseSingleSentence }
      .map{ sentence => Map(
        "insert_id" -> config.insertID,
        "document_id" -> sentence.documentID,
        "sentence_id" -> sentence.sentenceID,
        "text" -> sentence.text,
        "deps" -> sentence.deps
          .map{d => s"${d._2.deprel} >>> ${d._2.lemma} <<< ${sentence.deps(d._2.head).lemma}"}
        , "conll" -> sentence.deps.map(_._2).toList.sortBy(_.id).mkString("\n"))
      }
      .saveToEs(config.outputIndex)
  }

}


/* Mapping:

PUT depcc
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
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "lemma": {
              "type": "text",
              "analyzer": "english"
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
        "conll": {
          "type": "text",
          "index": false
        }
      }
    }
  },
  "settings": {
    "index": {
      "number_of_shards": "64",
      "number_of_replicas": "1"
    },
    "analysis": {
      "filter": {
        "english_stop": {
          "type": "stop",
          "stopwords": ""
        },
        "english_keywords": {
          "type": "keyword_marker",
          "keywords": [
            ""
          ]
        },
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        },
        "english_possessive_stemmer": {
          "type": "stemmer",
          "language": "possessive_english"
        }
      },
      "analyzer": {
        "english": {
          "tokenizer": "standard",
          "filter": [
            "english_possessive_stemmer",
            "lowercase",
            "english_stop",
            "english_keywords",
            "english_stemmer"
          ]
        }
      }
    }
  }
}

 */


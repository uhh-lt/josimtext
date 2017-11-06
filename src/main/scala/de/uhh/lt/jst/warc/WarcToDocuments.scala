package de.uhh.lt.jst.warc

import de.uhh.lt.jst.SparkJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

object WarcToDocuments extends SparkJob {

  val warcDocumentDelimiter = "WARC/1.0"
  val warcHeaderRegex = "^(WARC|c4_|Content-)".r
  val urlRegex = """WARC-Target-URI:\s+(.*)""".r
  val s3Regex = """c4_originalLocation:\s+(.*)""".r

  case class Config(
    inputDir: String = "",
    outputDir: String = ""
  )
  override type ConfigType = Config
  override val config = Config()
  override val description = ""
  override val parser = new Parser {

    arg[String]("INPUT_DIR").action( (x, c) =>
      c.copy(inputDir = x) ).required()
      .text(s"Directory with the WARC archives containing the documents: " +
        s"delimiter='$warcDocumentDelimiter'.")
    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDir = x) ).required().
      text(s"Directory with documents (one document per line) in the format" +
        s" '<url>\t<s3>\t<document-html>'.")
  }

  def getBody(document: String): String = {
    document
      .replace("\r", "")
      .split("\n")
      .filter { line => warcHeaderRegex.findFirstIn(line).isEmpty }
      .map { line => line.replaceAll("\\s+", " ") }
      .mkString("   ")


  }

  override def run(sc: SparkContext, config: Config): Unit = {
    val conf = new Configuration
    conf.set("textinputformat.record.delimiter", warcDocumentDelimiter)

    val unaggregatedFeatures: Unit = sc
      .newAPIHadoopFile(config.inputDir, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map { documentText =>

        val document = documentText._2.toString

        val url = {
          val urlMatch = urlRegex.findFirstMatchIn(document)
          if (urlMatch.isDefined) urlMatch.get.group(1).trim
          else ""
        }

        val s3 = {
          val s3Match = s3Regex.findFirstMatchIn(document)
          if (s3Match.isDefined) s3Match.get.group(1).trim
          else ""
        }

        val body = getBody(document)

        if (body.length > 1) s"$url\t$s3\t$body"
        else "-1"
      }
      .filter { line => line != "-1" }
      .saveAsTextFile(config.outputDir)
  }

}


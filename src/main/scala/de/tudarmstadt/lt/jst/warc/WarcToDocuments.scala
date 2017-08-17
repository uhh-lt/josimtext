package de.tudarmstadt.lt.jst.warc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import de.tudarmstadt.lt.jst.utils.Util
import de.tudarmstadt.lt.jst.verbs.Conll2Features.conllRecordDelimiter
import scala.util.matching.Regex

object WarcToDocuments {

  val warcDocumentDelimiter = "WARC/1.0"
  val warcHeaderRegex = "^(WARC|c4_|Content-)".r
  val urlRegex = """WARC-Target-URI:\s+(.*)""".r
  val s3Regex = """c4_originalLocation:\s+(.*)""".r

  def main(args: Array[String]) {
    if (args.size < 2) {
      println("Parameters: <input-dir> <output-dir>")
      println(s"<input-dir>\tDirectory with the WARC archives containing the documents: delimiter='$warcDocumentDelimiter'.")
      println(s"<output-dir>\tDirectory with documents (one document per line) in the format" +
        s" '<url>\t<s3>\t<document-html>'.")
      return
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    run(sc, inputPath, outputPath)
  }

  def getBody(document:String) = {
    document
      .replace("\r","")
      .split("\n")
      .filter{ line => warcHeaderRegex.findFirstIn(line).isEmpty }
      .map { line => line.replaceAll("\\s+", " ") }
      .mkString("   ")


  }

  def run(sc: SparkContext, inputDir:String, outputDir:String) = {
    val conf = new Configuration
    conf.set("textinputformat.record.delimiter", warcDocumentDelimiter)

    println("Input dir.: " + inputDir)
    println("Output dir.: " + outputDir)
    Util.delete(outputDir) // a convinience for the local tests

    val unaggregatedFeatures = sc
      .newAPIHadoopFile(inputDir, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map{ documentText =>

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
      .filter{ line => line != "-1"}
      .saveAsTextFile(outputDir)
  }
}


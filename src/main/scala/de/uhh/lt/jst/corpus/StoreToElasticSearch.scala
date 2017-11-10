package de.uhh.lt.jst.corpus

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object StoreToElasticSearch {

  val textRegex = """# text = (.*)""".r
  val newdocRegex = """# newdoc""".r
  val indexName = "spark/index"

  def main(args: Array[String]) {
    if (args.size < 1) {
      println("Parameters: <input-dir> <output-dir>")
      println(s"<input-dir>\tDirectory with a parsed corpus in the CoNLL format.")
      return
    }
    val inputPath = args(0)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)
    run(sc, inputPath)
  }


  def getText(line:String): String = {
    val textMatch = textRegex.findFirstMatchIn(line)
    if (textMatch.isDefined) textMatch.get.group(1).trim
    else line
  }

  def addDocumentBreaks(line:String): String = {
    val textMatch = newdocRegex.findFirstMatchIn(line)
    if (textMatch.isDefined) "\n\n" + line
    else line
  }

  var i = 1
  def getDocumentMap(line: String): Map[String, String] = {
    i = i+1
    val entry = Map("index" ->i.toString, "text" ->line)
    entry
  }

  def run(sc: SparkContext, inputConllDir: String) = {
    println("Input dir.: " + inputConllDir)

   sc
      .textFile(inputConllDir)
      .filter { line => line.startsWith("# ")}
      .filter{ line => !line.startsWith("# parser") && !line.startsWith("# sent_id")}
      .map{ line => getText(line)}
      .map{ line => addDocumentBreaks(line)}
      .map{ line => getDocumentMap(line)}
      .saveToEs(indexName)

  }
}

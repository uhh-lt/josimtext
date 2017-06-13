package warc

import org.apache.spark.{SparkConf, SparkContext}
import utils.Util

object WarcToDocuments {

  val warcDocumentDelimiter = "^WARC/1.0$".r

  def main(args: Array[String]) {
    if (args.size < 2) {
      println("Parameters: <input-dir> <output-dir>")
      println(s"<input-dir>\tDirectory with a parsed corpus in the CoNLL format: delimiter='$warcDocumentDelimiter'.")
      return
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    run(sc, inputPath, outputPath)
  }

  def run(sc: SparkContext, inputConllDir:String, outputConllDir:String) = {
    println("Input dir.: " + inputConllDir)
    println("Output dir.: " + outputConllDir)
    Util.delete(outputConllDir) // a convinience for the local tests

    sc
      .textFile(inputConllDir)
      //.map{ line => warcDocumentDelimiter.replaceAllIn(line, newConllRecordDelimiter) }
      .saveAsTextFile(outputConllDir)
  }
}


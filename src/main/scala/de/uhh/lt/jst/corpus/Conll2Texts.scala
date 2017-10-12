package de.uhh.lt.jst.corpus

import de.uhh.lt.jst.utils.Util
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

object Conll2Texts {
  def main(args: Array[String]) {
    if (args.size < 2) {
      println("Parameters: <input-dir> <output-dir>")
      println(s"<input-dir>\tDirectory with a parsed corpus in the CoNLL format.")
      println(s"<output-dir>\tDirectory with a corpus text format derived from CoNLL " +
        s"(but without any linguistic annotation)")
      return
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    run(sc, inputPath, outputPath)
  }

  val textRegex = """# text = (.*)""".r

  def getText(line:String): String = {
    val textMatch = textRegex.findFirstMatchIn(line)
    if (textMatch.isDefined) textMatch.get.group(1).trim
    else line
  }

  def run(sc: SparkContext, inputConllDir: String, outputConllDir: String) = {
    println("Input dir.: " + inputConllDir)
    println("Output dir.: " + outputConllDir)
    Util.delete(outputConllDir) // a convinience for the local tests

    sc
      .textFile(inputConllDir)
      .filter { line => line.startsWith("# ")}
      .filter{ line => !line.startsWith("# parser") && !line.startsWith("# sent_id")}
      .map{ line => getText(line)}
      .saveAsTextFile(outputConllDir, classOf[GzipCodec])
  }
}

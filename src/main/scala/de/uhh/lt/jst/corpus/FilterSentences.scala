package de.uhh.lt.jst.corpus


import de.uhh.lt.jst.utils.Util
import org.apache.spark.{SparkConf, SparkContext}

object FilterSentences {

  def main(args: Array[String]) {
    if (args.size < 2) {
      println("Remove noisy sentences not useful for dependency parsing.")
      println("Usage: FilterSentences <input-dir> <output-dir>")
      println("<input-dir>\tDirectory with corpus, strictly one sentence per line.")
      println("<output-dir>\tDirectory with output filtered sentences one per line.")
      return
    }

    val inputPath = args(0)
    val outputPath = args(1)
    Util.delete(outputPath) // convinience for local tests

    val conf = new SparkConf().setAppName("FilterSentences")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    run(sc, inputPath, outputPath)
  }

  def run(sc: SparkContext, inputDir: String, outputDir: String) = {
    println("Input dir: " + inputDir)
    println("Output dir: " + outputDir)

    val urlRegex = "(http://|www\\.|[a-z0-9]\\.com)".r
    val htmlRegex = "<[a-z ='\"/:0-9]+[^>]*>".r
    val latinTextRegex = "^[#±§-‒–—―©®™½¾@€£$¥&\u20BD\u00A0\u00AD%\\[\\])(（）;:,\\..?!\"'×Þß÷þøA-zÀ-ÿćęłńóśźżĄĆĘŁŃÓŚŹŻ0-9\\s-\\t/+α-ωΑ-Ω-]+$".r
    val someLettersRegex = "[A-z]+".r


    Util.delete(outputDir)
    sc.textFile(inputDir)
      .filter { line => htmlRegex.findFirstIn(line).isEmpty }
      .filter { line => urlRegex.findFirstIn(line).isEmpty }
      .filter { line => someLettersRegex.findFirstIn(line).isDefined }
      .filter { line => latinTextRegex.findFirstIn(line).isDefined }
      .saveAsTextFile(outputDir)
  }
}

package de.uhh.lt.jst.corpus


import de.uhh.lt.jst.SparkJob
import org.apache.spark.SparkContext

object FilterSentences extends SparkJob {

  case class Config(
    inputDir: String = "",
    outputDir: String = ""
  )

  override type ConfigType = Config
  override val config = Config()

  override val description: String = "Remove noisy sentences not useful for dependency parsing."

  override val parser = new Parser {

    arg[String]("INPUT_DIR").action( (x, c) =>
      c.copy(inputDir = x) ).required().
      text("Directory with corpus, strictly one sentence per line.")

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDir = x) ).required().
      text("Directory with output filtered sentences one per line.")
  }

  override def run(sc: SparkContext, config: Config): Unit = {

    val urlRegex = "(http://|www\\.|[a-z0-9]\\.com)".r
    val htmlRegex = "<[a-z ='\"/:0-9]+[^>]*>".r
    val latinTextRegex = "^[#±§-‒–—―©®™½¾@€£$¥&\u20BD\u00A0\u00AD%\\[\\])(（）;:,\\..?!\"'×Þß÷þøA-zÀ-ÿćęłńóśźżĄĆĘŁŃÓŚŹŻ0-9\\s-\\t/+α-ωΑ-Ω-]+$".r
    val someLettersRegex = "[A-z]+".r

    sc.textFile(config.inputDir)
      .filter { line => htmlRegex.findFirstIn(line).isEmpty }
      .filter { line => urlRegex.findFirstIn(line).isEmpty }
      .filter { line => someLettersRegex.findFirstIn(line).isDefined }
      .filter { line => latinTextRegex.findFirstIn(line).isDefined }
      .saveAsTextFile(config.outputDir)
  }
}

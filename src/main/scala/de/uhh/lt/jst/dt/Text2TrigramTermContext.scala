package de.uhh.lt.jst.dt

import de.uhh.lt.conll.CoNLLParser
import org.apache.spark.sql.{Dataset, SparkSession}

object Text2TrigramTermContext {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: input-file output-dir")
      return
    }

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    import spark.implicits._

    val input = args(0)
    val outputDir = args(1)

    val df = convertWithSpark(input)

    df.map(tc => s"${tc.term}\t${tc.context}")
      .write
      .text(outputDir)

  }

  // Note: `type TermContext = (String, String)` didn't work because of SPARK-12777
  case class TermContext(term: String, context: String)

  def text2TrigramTermContext(text: String): Seq[TermContext] = {
    val tokens = text.toLowerCase.split("\\s+").toSeq
    tokens.sliding(3).flatMap {
      case Seq(w1, w2, w3) => Some(TermContext(w2, w1 + "_@_" + w3))
      case _ => None // If we have less than three tokens, sliding(3) will provide a
      // Seq which has fewer than three elements
    }.toSeq
  }

  def convertWithSpark(path: String)(implicit spark: SparkSession): Dataset[TermContext] = {
    import spark.implicits._

    val text2Trigram = (text: String) => CoNLLParser.parseSingleSentence(text).comments

    val ds = spark.read.text(path)
      .flatMap(text => text2TrigramTermContext(text.getAs("value")))

    ds
  }
}

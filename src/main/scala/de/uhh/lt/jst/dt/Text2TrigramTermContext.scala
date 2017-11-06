package de.uhh.lt.jst.dt

import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.dt.entities.TermContext
import org.apache.spark.sql.{Dataset, SparkSession}

object Text2TrigramTermContext  extends SparkJob {

  case class Config(input: String = "", output: String = "")

  type ConfigType = Config
  override val config = Config()

  override val command: String = "Corpus2TrigramTermContext"
  override val description = "Extract trigrams from a text corpus and outputs a Term Context file"

  override val parser = new Parser {
    arg[String]("CORPUS_FILE").action( (x, c) =>
      c.copy(input = x) ).required().hidden()

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(output = x) ).required().hidden()
  }

  override def run(spark: SparkSession, config: Config): Unit = {

    import spark.implicits._

    val df = convertWithSpark(spark, config.input)

    df.map(tc => s"${tc.term}\t${tc.context}")
      .write
      .text(config.output)

  }

  def text2TrigramTermContext(text: String): Seq[TermContext] = {

    val removeTrailingPunctuations = (str: String) =>
        Set(",", ";", ".").fold(str){ (res, char) => res.stripSuffix(char)}

    val tokens = text
      .toLowerCase
      .split("\\s+")
      .map(s => if (s.length > 1) removeTrailingPunctuations(s) else s)
      .toSeq

    tokens.sliding(3).flatMap {
      case Seq(w1, w2, w3) => Some(TermContext(w2, w1 + "_@_" + w3))
      case _ => None // If we have less than three tokens, sliding(3) will provide a
      // Seq which has fewer than three elements
    }.toSeq
  }

  def convertWithSpark(spark: SparkSession, path: String): Dataset[TermContext] = {
    import spark.implicits._

    val ds = spark.read.text(path)
      .flatMap(text => text2TrigramTermContext(text.getAs("value")))

    ds
  }
}

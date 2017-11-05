package de.uhh.lt.jst.dt

import de.uhh.lt.jst.Job
import de.uhh.lt.jst.dt.entities.TermContext
import org.apache.spark.sql.{Dataset, SparkSession}

object Text2TrigramTermContext  extends Job {

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

  def run(config: Config): Unit =
    oldMain(config.productIterator.map(_.toString).toArray)

  // ------ unchanged old logic ------- //

  def oldMain(args: Array[String]): Unit = {

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

  def convertWithSpark(path: String)(implicit spark: SparkSession): Dataset[TermContext] = {
    import spark.implicits._

    val ds = spark.read.text(path)
      .flatMap(text => text2TrigramTermContext(text.getAs("value")))

    ds
  }
}

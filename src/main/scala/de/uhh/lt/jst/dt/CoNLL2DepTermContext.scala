package de.uhh.lt.jst.dt

import de.uhh.lt.conll.{CoNLLParser, Row, Sentence}
import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.dt.entities.TermContext
import de.uhh.lt.spark.corpus._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, SparkSession}



object CoNLL2DepTermContext extends SparkJob {
  case class Config(input: String = "", output: String = "")

  override type ConfigType = Config
  override val config = Config()

  override val command: String = "CoNLL2DepTermContext"
  override val description = "Extract dependencies from a CoNLL file and converts into Term Context files"

  override val parser = new Parser {
    arg[String]("CONLL_FILE").action( (x, c) =>
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

  def convertWithSpark(
    spark: SparkSession,
    path: String,
    autoDetectEnhancedDeps: Boolean = true
  ): Dataset[TermContext] = {

    import spark.implicits._

    // FIXME Avoid duplicate parsing
    val readConllCommentsUDF = udf((text: String) => CoNLLParser.parseSingleSentence(text).comments)
    val readConllRowsUDF = udf((text: String) => CoNLLParser.parseSingleSentence(text).rows)

    // TODO: why error with https://issues.scala-lang.org/browse/SI-6996 maybe Nil usage in extractor?
    val ds = spark.read.corpus(path)
      .withColumn("comments", readConllCommentsUDF('value))
      .withColumn("rows", readConllRowsUDF('value))
      .select("comments", "rows")
      .as[Sentence]

    /**
      * Some parsers will not provide enhanced dependencies.
      * In this case the DEPS column is always empty (either with "_" token or just empty).
      * In the default case (`autoDetectEnhancedDeps` is set to true) we try to detect
      * enhanced dependencies in the DEPS column and fallback to use normal dependencies
      * if they are NOT present by combining HEAD and DEPREL column.
      *
      */
    if(autoDetectEnhancedDeps && detectDatasetEnhancedDeps(ds))
      ds.flatMap(extractEnhancedDepForRows)
    else
      ds.flatMap(extractNormalDepsForSentence)
  }

  def detectDatasetEnhancedDeps(dataset: Dataset[Sentence]): Boolean =
    !detectDatasetMissesEnhancedDeps(dataset)

  def detectDatasetMissesEnhancedDeps(dataset: Dataset[Sentence]): Boolean = {
    val peekSize = 10 // Number of CoNLL sample sentences to check on
    val samples = dataset.take(peekSize)
    // If all samples sentences are without enhanced deps, we assume whole dataset misses them
    samples.forall(isSentenceWithoutEnhancedDeps)
  }

  def isSentenceWithoutEnhancedDeps(sentence: Sentence): Boolean = sentence.rows.forall(_.depsIsEmpty)

  def extractNormalDepsForSentence(sentence: Sentence): Seq[TermContext] = {
    val isIgnoredDepType = (row: Row) => Set("ROOT") contains row.deprel
    val rows = sentence.rows

    rows
      .filterNot(isIgnoredDepType)
      .flatMap { row =>
        val id = row.id.toInt
        val dep = extractNormalDepForId(sentence, id)
        val invDep = extractNormalDepForId(sentence, id, inverse = true)
        Seq(dep, invDep)
      }
  }

  def extractNormalDepForId(sentence: Sentence, id: Int, inverse: Boolean = false): TermContext = {
    val depRow = sentence.rows(id)

    val headId = depRow.head.toInt
    val headLemma = sentence.rows(headId.toInt).lemma
    val depLemma = depRow.lemma
    val depRel = depRow.deprel

    if (!inverse)
      TermContext(depLemma, s"$depRel#$headLemma")
    else
      TermContext(headLemma, s"-$depRel#$depLemma")
  }

  def extractEnhancedDepForRows(sentence: Sentence): Seq[TermContext] = {
    val isIgnoredDepType = (row: Row) => Set("ROOT") contains row.deprel

    val rows = sentence.rows

    val optDeps = rows
      .filterNot(isIgnoredDepType)
      .flatMap { row =>
        val id = row.id.toInt
        if (row.deps == "_") {
          None
        } else {
          val dep = extractEnhancedDepForId(sentence, id)
          val invDep = extractEnhancedDepForId(sentence, id, inverse = true)
          Some(Seq(dep, invDep))
        }
      }

    optDeps.flatten
  }


  def extractEnhancedDepForId(sentence: Sentence, id: Int, inverse: Boolean = false): TermContext = {
    val depRow = sentence.rows(id)

    assert(!depRow.depsIsEmpty, s"CoNLL row #$id does not contain enhanced deps.")

    val depRegex = raw"(\d+):(.+)".r

    depRow.deps match {
      case depRegex(headId, depRel) =>

        val headLemma = sentence.rows(headId.toInt).lemma
        val depLemma = depRow.lemma

        if (!inverse)
          TermContext(depLemma, s"$depRel#$headLemma")
        else
          TermContext(headLemma, s"-$depRel#$depLemma")
    }
  }

}

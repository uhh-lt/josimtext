package de.uhh.lt.jst.dt

import de.uhh.lt.conll.{CoNLLParser, Dependency, Sentence}
import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.dt.entities.TermContext
import de.uhh.lt.spark.corpus._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, SparkSession}


object CoNLL2DepTermContext extends SparkJob {
  case class Config(input: String = "", output: String = "")
  override type ConfigType = Config
  override val config = Config()
  override val description = "Extract term features from a CoNLL file and saves them into a Term Context file"

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

  def convertWithSpark(spark: SparkSession, path: String, autoDetectEnhancedDeps: Boolean = true): Dataset[TermContext] = {
    import spark.implicits._

    val readConllDeps = udf((text: String) => CoNLLParser.parseSingleSentence(text).deps)
    val readConllDocid = udf((text: String) => CoNLLParser.parseSingleSentence(text).documentID)
    val readConllSentid = udf((text: String) => CoNLLParser.parseSingleSentence(text).sentenceID)
    val readConllText = udf((text: String) => CoNLLParser.parseSingleSentence(text).text)

    val ds = spark.read.corpus(path)
      .withColumn("deps", readConllDeps('value))
      .withColumn("documentID", readConllDocid('value))
      .withColumn("sentenceID", readConllSentid('value))
      .withColumn("text", readConllText('value))
      .select("deps", "documentID", "sentenceID", "text")
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

  def isSentenceWithoutEnhancedDeps(sentence: Sentence): Boolean = sentence.deps.forall(_._2.depsIsEmpty)

  def extractNormalDepsForSentence(sentence: Sentence): Seq[TermContext] = {
    val isIgnoredDepType = (row: Dependency) => Set("ROOT") contains row.deprel
    val rows = sentence.deps

    rows
      .map(_._2)
      .filterNot{isIgnoredDepType}
      .flatMap { row =>
        val id = row.id
        val dep = extractNormalDepForId(sentence, id)
        val invDep = extractNormalDepForId(sentence, id, inverse = true)
        Seq(dep, invDep)
      }
      .toSeq
  }

  def extractNormalDepForId(sentence: Sentence, id: Int, inverse: Boolean = false): TermContext = {
    val depRow = sentence.deps(id)

    val headId = depRow.head.toInt
    val headLemma = sentence.deps(headId.toInt).lemma
    val depLemma = depRow.lemma
    val depRel = depRow.deprel

    if (!inverse)
      TermContext(depLemma, s"$depRel#$headLemma")
    else
      TermContext(headLemma, s"-$depRel#$depLemma")
  }

  def extractEnhancedDepForRows(sentence: Sentence): Seq[TermContext] = {
    val isIgnoredDepType = (row: Dependency) => Set("ROOT") contains row.deprel

    val rows = sentence.deps

    val optDeps = rows
        .map(_._2)
      .filterNot{isIgnoredDepType}
      .flatMap { row =>
        val id = row.id
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
    .toSeq

  def extractEnhancedDepForId(sentence: Sentence, id: Int, inverse: Boolean = false): TermContext = {
    val depRow = sentence.deps(id)

    assert(!depRow.depsIsEmpty, s"CoNLL row #$id does not contain enhanced deps.")

    val depRegex = raw"(\d+):(.+)".r

    depRow.deps match {
      case depRegex(headId, depRel) =>

        val headLemma = sentence.deps(headId.toInt).lemma
        val depLemma = depRow.lemma

        if (!inverse)
          TermContext(depLemma, s"$depRel#$headLemma")
        else
          TermContext(headLemma, s"-$depRel#$depLemma")
    }
  }

}

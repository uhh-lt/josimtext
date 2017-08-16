package dt

import de.tudarmstadt.lt.conll.{CoNLLParser, Row, Sentence}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{explode, udf}
import de.tudarmstadt.lt.spark.corpus._

object CoNLL2DepTermContext {

  // Note: `type TermContext = (String, String)` didn't work because of SPARK-12777
  case class TermContext(term: String, context: String)

  def convertWithSpark(path: String)(implicit spark: SparkSession): Dataset[TermContext] = {
    import spark.implicits._

    // FIXME
    val readConllCommentsUDF = udf((text: String) => CoNLLParser.parseSingleSentence(text).comments)
    val readConllRowsUDF = udf((text: String) => CoNLLParser.parseSingleSentence(text).rows)

    // TODO: why error with https://issues.scala-lang.org/browse/SI-6996 maybe Nil usage in extractor?
    val ds = spark.read.corpus(path)
      .withColumn("comments", readConllCommentsUDF('value))
      .withColumn("rows", readConllRowsUDF('value))
      .select("comments", "rows")
      .as[Sentence]
      .flatMap(s => extractDepTermContextPairs(s.rows))

    ds
  }

  def extractDepTermContextPairs(rows: Seq[Row]): Seq[TermContext] = {
    rows.flatMap { row =>
      val id = row.id.toInt
      if(row.deps == "_") {
        None
      } else {
        val dep = extractDepTermContextForId(rows, id)
        val invDep = extractDepTermContextForId(rows, id, inverse = true)
        Some(Seq(dep, invDep))
      }
    }.flatten
  }


  def extractDepTermContextForId(rows: Seq[Row], id: Int, inverse: Boolean = false): TermContext = {
    val depRow = rows(id)

    assert(depRow.deps != "_")

    val depRegex = raw"(\d+):(\w+)".r

    depRow.deps match {
      case depRegex(headId, depRel) =>

        val headLemma = rows(headId.toInt).lemma
        val depLemma = depRow.lemma

        if (! inverse)
          TermContext(depLemma, s"$depRel#$headLemma")
        else
          TermContext(headLemma, s"-$depRel#$depLemma")
    }

  }
}

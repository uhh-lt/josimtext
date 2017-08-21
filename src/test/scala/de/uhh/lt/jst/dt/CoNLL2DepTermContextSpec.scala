package de.uhh.lt.jst.dt

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.FlatSpec
import de.uhh.lt.conll.{CoNLLParser, Row, Sentence}
import de.uhh.lt.jst.dt.CoNLL2DepTermContext.TermContext

import scala.io.Source

class CoNLL2DepTermContextSpec extends FlatSpec with DatasetSuiteBase {

  val expectedTermContextPairs: Seq[TermContext] = Seq(
    ("Website", "nn#Tips"),
    ("Tips", "-nn#Website"),
    ("Usability", "nn#Tips"),
    ("Tips", "-nn#Usability"),
    ("Tips", "ROOT#Tips"),
    ("Tips", "-ROOT#Tips"),
    (",", "punct#Tips"),
    ("Tips", "-punct#,"),
    ("Tricks", "conj_and#Tips"),
    ("Tips", "-conj_and#Tricks"),
    ("Mistakes", "conj_and#Tips"),
    ("Tips", "-conj_and#Mistakes"),
    (".", "punct#Tips"),
    ("Tips", "-punct#.")
  ).map(TermContext.apply _ tupled _)



  it should "extract dependency term context pairs from CoNLL file" in {
    val path = Option(this.getClass.getResource("/conll.csv")).map(_.getPath).get
    val text = Source.fromFile(path).mkString

    val sentence = CoNLLParser.parseSingleSentence(text)
    val deps = CoNLL2DepTermContext.extractDepTermContextPairs(sentence.rows)

    assert(deps, expectedTermContextPairs)
  }

  "Spark" should "read a CoNLL file and convert it to dependency term context pairs" in {

    import spark.implicits._

    val path = this.getClass.getResource("/conll.csv").getPath
    val result = CoNLL2DepTermContext.convertWithSpark(path)(spark)

    val expected = sc.parallelize(expectedTermContextPairs).toDS

    assertDatasetEquals(expected, result)
  }

  it should "extract correctly from deps with non alphanum tokens" in {
    val rows: Seq[Row] = Seq(Row(
      id = "0",
      form = "usr/lib/fglrx",
      lemma = "usr/lib/fglrx",
      upostag = "NN",
      xpostag = "NN",
      feats = "",
      head = "0",
      deprel = "pobj",
      deps = "0:prep_usr/", // slashes do appear in the data
      misc = "O"
    ))

    val deps = CoNLL2DepTermContext.extractDepTermContextPairs(rows)
    val expected = Seq(
      TermContext("usr/lib/fglrx","prep_usr/#usr/lib/fglrx"),
      TermContext("usr/lib/fglrx","-prep_usr/#usr/lib/fglrx")
    )
    assert(deps, expected)
  }
}

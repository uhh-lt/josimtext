package de.uhh.lt.jst.dt

import java.io.FileNotFoundException

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import de.uhh.lt.conll.{CoNLLParser, Dependency, Sentence}
import de.uhh.lt.jst.dt.entities.TermContext

import scala.io.Source

class CoNLL2DepTermContextSpec extends FlatSpec with Matchers with DatasetSuiteBase {
  def getResourcePath(var1: String): String = {
    val someResource = Option(this.getClass.getResource(var1))
    someResource match {
      case Some(resource) => resource.getPath
      case None => throw new FileNotFoundException(s"Resource not found: $var1")
    }
  }

  val expectedEnhancedDeps: Seq[TermContext] = Seq(
    ("Website", "nn#Tips"),
    ("Tips", "-nn#Website"),
    ("Usability", "nn#Tips"),
    ("Tips", "-nn#Usability"),
    (",", "punct#Tips"),
    ("Tips", "-punct#,"),
    ("Tricks", "conj_and#Tips"),
    ("Tips", "-conj_and#Tricks"),
    ("Mistakes", "conj_and#Tips"),
    ("Tips", "-conj_and#Mistakes"),
    (".", "punct#Tips"),
    ("Tips", "-punct#.")
  ).map(TermContext.apply _ tupled _)

  val expectedNormalDeps: Seq[TermContext] = Seq(
    ("Website", "nn#Tips"),
    ("Tips", "-nn#Website"),
    ("Usability", "nn#Tips"),
    ("Tips", "-nn#Usability"),
    (",", "punct#Tips"),
    ("Tips", "-punct#,"),
    ("Tricks", "conj#Tips"),
    ("Tips", "-conj#Tricks"),
    ("and", "cc#Tips"),
    ("Tips", "-cc#and"),
    ("Mistakes", "conj#Tips"),
    ("Tips", "-conj#Mistakes"),
    (".", "punct#Tips"),
    ("Tips", "-punct#.")
  ).map(TermContext.apply _ tupled _)

  it should "extract dependency term context pairs from CoNLL file" in {
    val path = getResourcePath("/conll.csv")
    val text = Source.fromFile(path).mkString

    val sentence = CoNLLParser.parseSingleSentence(text)
    val deps = CoNLL2DepTermContext.extractEnhancedDepForRows(sentence)

    assert(deps.toSet, expectedEnhancedDeps.toSet)
  }

  it should "detect that CoNLL file does not contain enhanced dependencies" in {
    val path = getResourcePath("/conll-wo-enhanced-deps.csv")
    val text = Source.fromFile(path).mkString

    val sentence = CoNLLParser.parseSingleSentence(text)

    CoNLL2DepTermContext.isSentenceWithoutEnhancedDeps(sentence) should be (true)
  }

  "Spark" should "read a CoNLL file and convert it to dependency term context pairs" in {
    import spark.implicits._
    val path = getResourcePath("/conll.csv")
    val result = CoNLL2DepTermContext.convertWithSpark(spark, path)

    val expected = sc.parallelize(expectedEnhancedDeps).toDS
    assert(expected.collect().toSet, result.collect().toSet)
  }

  "Spark" should "detect that a CoNLL file misses enhanced deps and extract normal deps" in {
    import spark.implicits._

    val path = getResourcePath("/conll-wo-enhanced-deps.csv")
    val result = CoNLL2DepTermContext.convertWithSpark(spark, path)

    val expected = sc.parallelize(expectedNormalDeps).toDS
    assert(expected.collect().toSet, result.collect().toSet)
  }

  it should "extract correctly from deps with non alphanum tokens" in {
    val rows: Seq[Dependency] = Seq(Dependency(
      id = 0,
      form = "usr/lib/fglrx",
      lemma = "usr/lib/fglrx",
      upostag = "NN",
      xpostag = "NN",
      feats = "",
      head = 0,
      deprel = "pobj",
      deps = "0:prep_usr/", // slashes do appear in the data
      ner = "O"
    ))



    val deps = CoNLL2DepTermContext.extractEnhancedDepForRows(Sentence(
      rows.map(_.id).zip(rows).toMap))
    val expected = Seq(
      TermContext("usr/lib/fglrx","prep_usr/#usr/lib/fglrx"),
      TermContext("usr/lib/fglrx","-prep_usr/#usr/lib/fglrx")
    )
    assert(deps.toSet, expected.toSet)
  }
}

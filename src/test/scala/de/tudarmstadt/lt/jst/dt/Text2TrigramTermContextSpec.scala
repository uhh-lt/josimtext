package de.tudarmstadt.lt.jst.dt

import com.holdenkarau.spark.testing.DatasetSuiteBase
import de.tudarmstadt.lt.jst.dt.Text2TrigramTermContext.TermContext
import org.scalatest._

import scala.io.Source

class Text2TrigramTermContextSpec extends FlatSpec with Matchers with DatasetSuiteBase {

  val expectedTermContextPairsPerLine: Seq[Seq[TermContext]] = Seq(
    Seq(
      TermContext("world,","hello_@_we"),
      TermContext("we","world,_@_have"),
      TermContext("have","we_@_a"),
      TermContext("a","have_@_few"),
      TermContext("few","a_@_trigrams"),
      TermContext("trigrams","few_@_here.")
    ),
    Seq()
  )

  it should "calculate correct trigrams from text" in {
    val lines = Source.fromFile(textFilePath).getLines().toSeq

    val result = lines.map(Text2TrigramTermContext.text2TrigramTermContext)

    result should be(expectedTermContextPairsPerLine)
  }

  "Spark" should "calculate correct trigrams from text" in {
    import spark.implicits._

    val result = Text2TrigramTermContext.convertWithSpark(textFilePath)(spark)

    val expected = sc.parallelize(expectedTermContextPairsPerLine.flatten).toDS
    assertDatasetEquals(expected, result)
  }

  val textFilePath: String = Option(this.getClass.getResource("/hello-world.txt")).map(_.getPath).get
}

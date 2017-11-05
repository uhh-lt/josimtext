package de.uhh.lt.jst.corpus

import com.holdenkarau.spark.testing.SharedSparkContext
import de.uhh.lt.testing.tags.NeedsMissingFiles
import org.scalatest._

class Conll2TextsTest extends FlatSpec with Matchers  with SharedSparkContext {

  def run(inputPath:String) = {
    val outputPath = inputPath + "-output"
    Conll2Texts.run(sc, inputPath, outputPath)
  }

  it should "extract text from a conll file" in {
    val conllPath = getClass.getResource("/conll-1000-tokens.csv.gz").getPath()
    run(conllPath)
  }

  it should "extract text from line" in {
    Conll2Texts.getText("") shouldEqual("")
    Conll2Texts.getText("# text = ") shouldEqual("")
    Conll2Texts.getText("# text = a") shouldEqual("a")
    Conll2Texts.getText("# text = This is some text. ") shouldEqual("This is some text.")

}

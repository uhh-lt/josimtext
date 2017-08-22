package de.uhh.lt.jst.verbs

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import de.uhh.lt.jst.utils.Const
import de.uhh.lt.testtags.NeedsMissingFiles

class Conll2FeaturesTest extends FlatSpec with Matchers  with SharedSparkContext {

  def run(inputPath: String, verbsOnly:Boolean=false) = {
    val outputPath = inputPath + "-output"

    Conll2Features.run(sc, inputPath, outputPath, verbsOnly)
  }

  "simplify" should "simplify pos tag" in {
    Conll2Features.simplifyPos("NNS") should equal("NN")
    Conll2Features.simplifyPos("VBZ") should equal("VB")
  }

  // to run it first download the large test dataset, then uncomment:
  // wget http://panchenko.me/data/joint/verbs/part-m-00000.gz -O src/test/resources/conll-very-large.csv.gz
  ignore should "run large dataset" taggedAs NeedsMissingFiles in {
    val conllPath = getClass.getResource("conll-very-large.csv.gz").getPath()
    run(conllPath)
  }

  it should "run small dataset" in {
    val conllPath = getClass.getResource(Const.FeatureExtractionTests.conll).getPath()
    println(conllPath)
    run(conllPath)
  }
}
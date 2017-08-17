package de.uhh.lt.jst.verbs

import com.holdenkarau.spark.testing.SharedSparkContext
import de.uhh.lt.testtags.{BrokenTest, NeedsMissingFiles}
import org.scalatest._
import de.uhh.lt.jst.utils.Const

class Conll2FeaturesTest extends FlatSpec with Matchers  with SharedSparkContext {

  def run(inputPath: String, verbsOnly:Boolean=false) = {
    val outputPath = inputPath + "-output"

    Conll2Features.run(sc, inputPath, outputPath, verbsOnly)
  }

  "simplify" should "simplify pos tag" in {
    Conll2Features.simplifyPos("NNS") should equal("NN")
    Conll2Features.simplifyPos("VBZ") should equal("VB")
  }

  ignore should "run very large dataset verbs only" taggedAs NeedsMissingFiles in {
    val conllPath = "/Users/sasha/work/active/joint/JoSimText/src/test/resources/conll_large-output"
    //val conllPath = "/Users/panchenko/Desktop/conll-output"
    run(conllPath, true)
  }

  // Aborted with message:
  // java.lang.NullPointerException:
  // at de.uhh.lt.jst.verbs.Conll2FeaturesTest$$anonfun$3.apply$mcV$sp(Conll2FeaturesTest.scala:34)
  ignore should "run large dataset verbs only" taggedAs BrokenTest in {
    val conllPath = getClass.getResource("part-m-00000.gz").getPath()
    run(conllPath, true)
  }

  // Aborted with message:
  // java.lang.NullPointerException:
  // at de.uhh.lt.jst.verbs.Conll2FeaturesTest$$anonfun$3.apply$mcV$sp(Conll2FeaturesTest.scala:34)
  ignore should "run large dataset" taggedAs BrokenTest in {
    // wget http://panchenko.me/data/joint/verbs/part-m-00000.gz in src/test/resources
    val conllPath = getClass.getResource("t").getPath()
    run(conllPath)
  }

  // Aborted with message:
  // java.lang.NullPointerException:
  // at de.uhh.lt.jst.verbs.Conll2FeaturesTest$$anonfun$4.apply$mcV$sp(Conll2FeaturesTest.scala:39)
  ignore should "run small dataset" taggedAs BrokenTest in {
    val conllPath = getClass.getResource(Const.FeatureExtractionTests.conll).getPath()
    println(conllPath)
    run(conllPath)
  }
}
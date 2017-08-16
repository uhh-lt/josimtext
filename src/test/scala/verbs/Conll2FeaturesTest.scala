package verbs

import com.holdenkarau.spark.testing.SharedSparkContext
import de.tudarmstadt.lt.testtags.NeedsMissingFiles
import org.scalatest._
import utils.Const

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

  it should "run large dataset verbs only" in {
    val conllPath = getClass.getResource("part-m-00000.gz").getPath()
    run(conllPath, true)
  }

  "large dataset" should "run" in {
    // wget http://panchenko.me/data/joint/verbs/part-m-00000.gz in src/test/resources
    val conllPath = getClass.getResource("t").getPath()
    run(conllPath)
  }

  "small dataset" should "run" in {
    val conllPath = getClass.getResource(Const.FeatureExtractionTests.conll).getPath()
    println(conllPath)
    run(conllPath)
  }
}
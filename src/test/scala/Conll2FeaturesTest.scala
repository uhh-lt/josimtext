
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import utils.Const
import verbs.Conll2Features

class Conll2FeaturesTest extends FlatSpec with Matchers {

  def run(inputPath: String, verbsOnly:Boolean=false) = {
    val outputPath = inputPath + "-output"

    val conf = new SparkConf()
      .setAppName("test")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    Conll2Features.run(sc, inputPath, outputPath, verbsOnly)
  }

  "simplify" should "simplify pos tag" in {
    Conll2Features.simplifyPos("NNS") should equal("NN")
    Conll2Features.simplifyPos("VBZ") should equal("VB")
  }

  "very large dataset verbs only" should "run" in {
    //val conllPath = "/Users/sasha/work/active/joint/JoSimText/src/test/resources/conll_large-output"
    //val conllPath = "/Users/panchenko/Desktop/conll-output"
    //run(conllPath, true)
  }

  "large dataset verbs only" should "run" in {
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
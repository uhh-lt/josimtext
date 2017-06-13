package warc

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import utils.Const

class WarcToDocumentsTest extends FlatSpec with Matchers {

  def run(inputPath: String, verbsOnly:Boolean=false) = {
    val outputPath = inputPath + "-output"

    val conf = new SparkConf()
      .setAppName("test")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    WarcToDocuments.run(sc, inputPath, outputPath)
  }

  "large dataset" should "run" in {
    val conllPath = "/Users/sasha/work/active/joint/JoSimText/src/test/resources/conll_large-output"
    run(conllPath, true)
  }

  "small dataset" should "run" in {
    val path = getClass.getResource(Const.FeatureExtractionTests.warc).getPath()
    run(path)
  }
}
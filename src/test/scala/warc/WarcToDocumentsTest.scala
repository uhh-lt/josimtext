package warc

import com.holdenkarau.spark.testing.SharedSparkContext
import de.tudarmstadt.lt.testtags.NeedsMissingFiles
import org.scalatest._
import utils.Const

class WarcToDocumentsTest extends FlatSpec with Matchers  with SharedSparkContext {

  def run(inputPath: String, verbsOnly:Boolean=false) = {
    val outputPath = inputPath + "-output"

    WarcToDocuments.run(sc, inputPath, outputPath)
  }

  ignore should "run large dataset" taggedAs NeedsMissingFiles in {
    val conllPath = "/Users/sasha/work/active/joint/JoSimText/src/test/resources/conll_large-output"
    run(conllPath, true)
  }

  "small dataset" should "run" in {
    val path = getClass.getResource(Const.FeatureExtractionTests.warc).getPath()
    run(path)
  }
}
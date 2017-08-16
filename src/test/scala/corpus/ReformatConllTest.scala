package corpus

import com.holdenkarau.spark.testing.SharedSparkContext
import de.tudarmstadt.lt.testtags.NeedsMissingFiles
import org.scalatest._

class ReformatConllTest extends FlatSpec with Matchers  with SharedSparkContext {
  def run(inputPath:String) = {
    val outputPath = inputPath + "-output"
    ReformatConll.run(sc, inputPath, outputPath)
  }

  ignore should "run reformat standard" taggedAs NeedsMissingFiles in {
    val conllPath = getClass.getResource("/conll_orig").getPath()
    run(conllPath)
  }

  ignore should "run reformat large" taggedAs NeedsMissingFiles in {
    //val conllPath = "/Users/sasha/work/active/joint/JoSimText/src/test/resources/conll_large"
    val conllPath = "/Users/panchenko/Desktop/conll"
    run(conllPath)
  }
}

package dt

import com.holdenkarau.spark.testing.SharedSparkContext
import de.tudarmstadt.lt.testtags.NeedsMissingFiles
import org.scalatest._

class DTCaseFilterTest extends FlatSpec with Matchers with SharedSparkContext {

  def run(dtPath: String) = {
    val outputPath = dtPath + "-output"

    DTCaseFilter.run(sc, dtPath, outputPath)
  }

  ignore should "filter DT by vocabulary" taggedAs NeedsMissingFiles in {
    //val senses =  getClass.getResource(Const.PRJ.SENSES).getPath()
    run(dtPath = "/Users/alex/Desktop/w2v-jbt-nns/dt-text.csv")
  }
}


package de.tudarmstadt.lt.jst.dt

import com.holdenkarau.spark.testing.SharedSparkContext
import de.tudarmstadt.lt.testtags.NeedsMissingFiles
import org.scalatest._

class DTFilterTest extends FlatSpec with Matchers with SharedSparkContext {

  def run(dtPath: String, vocPath: String) = {
    val outputPath = dtPath + "-output"

    DTFilter.run(sc, dtPath, vocPath, outputPath, keepSingleWords = false, filterOnlyTarget = true)
  }

  ignore should "filter DT by vocabulary" taggedAs NeedsMissingFiles in {
    //val senses =  getClass.getResource(Const.PRJ.SENSES).getPath()
    run(dtPath = "/Users/alex/Desktop/w2v-jbt-nns/de.tudarmstadt.lt.jst.dt-text.csv",
      vocPath = "/Users/alex/Desktop/w2v-jbt-nns/target.csv")
  }
}
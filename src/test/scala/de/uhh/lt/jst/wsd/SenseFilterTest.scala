package de.uhh.lt.jst.wsd

import com.holdenkarau.spark.testing.SharedSparkContext
import de.uhh.lt.testtags.NeedsMissingFiles
import org.scalatest._

class SenseFilterTest extends FlatSpec with Matchers  with SharedSparkContext {

  def run(sensesPath:String, vocPath:String) = {
    val outputPath = sensesPath + "-output"

    SensesFilter.runCli(sc, sensesPath, vocPath, outputPath, lowercase = true, lowerOrFirstUpper = true)
  }

  ignore should "filter sense clusters by vocabulary" taggedAs NeedsMissingFiles in {
    //val senses =  getClass.getResource(Const.PRJ.SENSES).getPath()
    run(sensesPath="/Users/sasha/Desktop/w2v-jbt-nns/senses-test.csv",
      vocPath="/Users/sasha/Desktop/w2v-jbt-nns/voc.csv")
  }
}
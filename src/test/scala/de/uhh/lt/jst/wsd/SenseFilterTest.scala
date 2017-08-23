package de.uhh.lt.jst.wsd

import com.holdenkarau.spark.testing.SharedSparkContext
import de.uhh.lt.jst.utils.Const
import org.scalatest._

class SenseFilterTest extends FlatSpec with Matchers  with SharedSparkContext {

  def run(sensesPath:String, vocPath:String) = {
    val outputPath = sensesPath + "-output"

    SensesFilter.runCli(sc, sensesPath, vocPath, outputPath, lowercase = true, lowerOrFirstUpper = true)
  }

  it should "filter sense clusters by vocabulary" in {
    val sensesPath = getClass.getResource(Const.PRJ_TEST.SENSES).getPath()
    val vocPath = getClass.getResource("/voc-tiny.csv").getPath()
    run(sensesPath, vocPath)
  }
}
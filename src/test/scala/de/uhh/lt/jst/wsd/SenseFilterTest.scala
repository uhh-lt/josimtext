package de.uhh.lt.jst.wsd

import com.holdenkarau.spark.testing.DatasetSuiteBase
import de.uhh.lt.jst.utils.Const
import org.scalatest._

class SenseFilterTest extends FlatSpec with Matchers  with DatasetSuiteBase {

  def run(sensesPath:String, vocPath:String) = {
    val outputPath = sensesPath + "-output"
    val config = SensesFilter.Config(sensesPath, vocPath, outputPath, lowerCase = true, lowerOrFirstUpper = true)
    SensesFilter.run(spark, config)
  }

  it should "filter sense clusters by vocabulary" in {
    val sensesPath = getClass.getResource(Const.PRJ_TEST.SENSES).getPath()
    val vocPath = getClass.getResource("/voc-tiny.csv").getPath()
    run(sensesPath, vocPath)
  }
}
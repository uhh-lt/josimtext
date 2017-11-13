package de.uhh.lt.jst.wsd

import com.holdenkarau.spark.testing.DatasetSuiteBase
import de.uhh.lt.jst.utils.{Const, Util}
import org.scalatest._

class SenseFilterTest extends FlatSpec with Matchers  with DatasetSuiteBase {


  it should "filter sense clusters by vocabulary" in {
    val sensesPath = getClass.getResource(Const.PRJ_TEST.SENSES).getPath
    val vocPath = getClass.getResource("/voc-tiny.csv").getPath
    val outputPath = sensesPath + "-output"

    Util.delete(outputPath)

    val config = SensesFilter.Config(sensesPath, vocPath, outputPath, lowerCase = true, lowerOrFirstUpper = true)
    SensesFilter.run(spark, config)
  }
}
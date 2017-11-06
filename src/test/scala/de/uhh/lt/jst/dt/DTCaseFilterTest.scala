package de.uhh.lt.jst.dt

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest._

class DTCaseFilterTest extends FlatSpec with Matchers with DatasetSuiteBase {

  def run(dtPath: String): Unit = {
    val outputPath = dtPath + "-output"
    val config = DTCaseFilter.Config(dtPath, outputPath)
    DTCaseFilter.run(spark, config)
  }

  it should "filter DT by vocabulary" in {
    val dtPath =  getClass.getResource("/dt-tiny.csv").getPath
    run(dtPath)
  }
}


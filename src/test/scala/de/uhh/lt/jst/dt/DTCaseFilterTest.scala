package de.uhh.lt.jst.dt

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._

class DTCaseFilterTest extends FlatSpec with Matchers with SharedSparkContext {

  def run(dtPath: String) = {
    val outputPath = dtPath + "-output"

    DTCaseFilter.run(sc, dtPath, outputPath)
  }

  it should "filter DT by vocabulary" in {
    val dtPath =  getClass.getResource("/dt-tiny.csv").getPath()
    run(dtPath)
  }
}


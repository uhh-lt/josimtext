package de.uhh.lt.jst.warc

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest._
import de.uhh.lt.jst.utils.Const

class WarcToDocumentsTest extends FlatSpec with Matchers  with DatasetSuiteBase {
  def run(inputPath: String, verbsOnly:Boolean=false): Unit = {
    val outputPath = inputPath + "-output"
    val config = WarcToDocuments.Config(inputPath, outputPath)
    WarcToDocuments.run(spark, config)
  }

  "small dataset" should "run" in {
    val path = getClass.getResource(Const.FeatureExtractionTests.warc).getPath()
    run(path)
  }
}
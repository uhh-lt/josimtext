package de.uhh.lt.jst.warc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import de.uhh.lt.jst.utils.Const

class WarcToDocumentsTest extends FlatSpec with Matchers  with SharedSparkContext {
  def run(inputPath: String, verbsOnly:Boolean=false) = {
    val outputPath = inputPath + "-output"

    WarcToDocuments.run(sc, inputPath, outputPath)
  }

  "small dataset" should "run" in {
    val path = getClass.getResource(Const.FeatureExtractionTests.warc).getPath()
    run(path)
  }
}
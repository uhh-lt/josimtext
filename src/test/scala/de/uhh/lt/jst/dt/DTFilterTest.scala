package de.uhh.lt.jst.dt

import java.nio.file.Paths

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._

import scala.io.Source

class DTFilterTest extends FlatSpec with Matchers with SharedSparkContext {

  def run(dtPath: String, vocPath: String, keepSingleWords:Boolean) = {
    val outputDir = dtPath + "-output"
    DTFilter.run(sc, dtPath, vocPath, outputDir, keepSingleWords, filterOnlyTarget = true)
    outputDir
  }

  it should "filter DT by vocabulary (no singles)" in {
    val dtPath =  getClass.getResource("/dt-tiny.csv").getPath()
    val vocPath =  getClass.getResource("/voc-tiny.csv").getPath()
    val outputDir = run(dtPath, vocPath, false)
    Source.fromFile(Paths.get(outputDir + "/part-00000").toString).getLines.toList.length should equal(2)
    Source.fromFile(Paths.get(outputDir + "/part-00001").toString).getLines.toList.length should equal(3)
  }

  it should "filter DT by vocabulary (singles)" in {
    val dtPath =  getClass.getResource("/dt-tiny.csv").getPath()
    val vocPath =  getClass.getResource("/voc-tiny.csv").getPath()
    val outputDir = run(dtPath, vocPath, true)
    Source.fromFile(Paths.get(outputDir + "/part-00000").toString).getLines.toList.length should equal(4)
    Source.fromFile(Paths.get(outputDir + "/part-00001").toString).getLines.toList.length should equal(3)
  }

}
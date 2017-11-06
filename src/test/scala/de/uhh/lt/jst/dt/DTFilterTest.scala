package de.uhh.lt.jst.dt

import java.nio.file.Paths

import com.holdenkarau.spark.testing.SharedSparkContext
import de.uhh.lt.jst.utils.Util
import org.scalatest._

import scala.io.Source

class DTFilterTest extends FlatSpec with Matchers with SharedSparkContext {

  it should "filter DT by vocabulary (no singles)" in {
    val dtPath =  getClass.getResource("/dt-tiny.csv").getPath
    val vocPath =  getClass.getResource("/voc-tiny.csv").getPath

    val outputDir = dtPath + "-output"
    Util.delete(outputDir)
    val config = DTFilter.Config(dtPath, vocPath, outputDir,  keepSingleWords = false, filterOnlyTarget = true)
    DTFilter.run(sc, config)

    Source.fromFile(Paths.get(outputDir + "/part-00000").toString).getLines.toList.length should equal(2)
    Source.fromFile(Paths.get(outputDir + "/part-00001").toString).getLines.toList.length should equal(3)
  }

  it should "filter DT by vocabulary (singles)" in {
    val dtPath =  getClass.getResource("/dt-tiny.csv").getPath
    val vocPath =  getClass.getResource("/voc-tiny.csv").getPath

    val outputDir = dtPath + "-output"
    Util.delete(outputDir)

    val config = DTFilter.Config(dtPath, vocPath, outputDir)
    DTFilter.run(sc, config)

    Source.fromFile(Paths.get(outputDir + "/part-00000").toString).getLines.toList.length should equal(4)
    Source.fromFile(Paths.get(outputDir + "/part-00001").toString).getLines.toList.length should equal(3)
  }

}
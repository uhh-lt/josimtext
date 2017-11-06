package de.uhh.lt.jst.wsd


import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import de.uhh.lt.jst.utils.{Const, Util}

class Clusters2FeaturesTest extends FlatSpec with Matchers with SharedSparkContext {
  "The Cluster2Features object" should "generate result" in {
    val sensesPath = getClass.getResource(Const.PRJ_TEST.SENSES).getPath()
    val outputPath = sensesPath + "-output"
    Util.delete(outputPath)
    val config = Clusters2Features.Config(sensesPath, outputPath)
    Clusters2Features.run(sc, config)
  }
}





package wsd


import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import utils.Const

class Clusters2FeaturesTest extends FlatSpec with Matchers with SharedSparkContext {
  "The Cluster2Features object" should "generate result" in {
    val sensesPath = getClass.getResource(Const.PRJ_TEST.SENSES).getPath()
    val outputPath = sensesPath + "-output"

    Clusters2Features.run(sensesPath, outputPath, sc)
  }
}





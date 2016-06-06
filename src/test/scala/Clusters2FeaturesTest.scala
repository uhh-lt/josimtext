

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class Clusters2FeaturesTest extends FlatSpec with ShouldMatchers {
    "The Cluster2Features object" should "generate result" in {
        val sensesPath =  getClass.getResource(Const.PRJ.SENSES).getPath()
        val outputPath =  sensesPath + "-output"

        val conf = new SparkConf()
            .setAppName("JST: Clusters2Features")
            .setMaster("local[4]")
        val sc = new SparkContext(conf)
        Clusters2Features.run(sensesPath, outputPath, sc)
    }
}





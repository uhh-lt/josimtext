import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class WSDTest extends FlatSpec with ShouldMatchers {
    "The Cluster2Features object" should "generate result" in {

        val senses =  getClass.getResource(Const.PRJ.SENSES).getPath()
        val output =  senses + "-output"
        val resDir = Const.PRJ.RES_DIR
        val clusters = resDir + "/clusters.csv"
        val coocs = resDir + "/coocs.csv"
        val trigrams = resDir + "/trigrams.csv"
        val deps = resDir + "/deps.csv"
        val contexts = resDir + "/contexts.csv"
        val mode = WSDFeatures.DepstargetCoocsClusters

        val conf = new SparkConf()
          .setAppName("JST: Clusters2Features")
          .setMaster("local[4]")
        val sc = new SparkContext(conf)

        WSD.run(contexts, output, clusters, coocs, deps, true, mode, 20000, 1, sc)
  }
}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class WSDTest extends FlatSpec with ShouldMatchers {

    def wsd(mode: WSDFeatures.Value) = {
        val senses =  getClass.getResource(Const.PRJ.SENSES).getPath()
        val output =  senses + "-output"
        val resDir = Const.PRJ.RES_DIR
        val clusters = resDir + "/clusters.csv"
        val coocs = resDir + "/depwords.csv"
        val trigrams = resDir + "/trigrams.csv"
        val deps = resDir + "/deps.csv"
        val contexts = resDir + "/contexts.csv"

        val conf = new SparkConf()
            .setAppName("JST: WSD")
            .setMaster("local[1]")
        val sc = new SparkContext(conf)

        WSD.run(sc, contexts, output, clusters, coocs, deps, trigrams, true, mode, 20000, 1)
    }

//    "DepstargetCoocsClustersTrigramstarget" should "run" in {
//        wsd(WSDFeatures.DepstargetCoocsClustersTrigramstarget)
//    }

//    "Depstarget" should "run" in {
//        wsd(WSDFeatures.Depstarget)
//    }

    "Clusters" should "run" in {
        wsd(WSDFeatures.Clusters)
    }
}
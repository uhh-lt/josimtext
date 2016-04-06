import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class WSDTest extends FlatSpec with ShouldMatchers {

    def wsd(mode: WSDFeatures.Value) = {
        val senses =  getClass.getResource(Const.PRJ.SENSES).getPath()
        val output =  senses + "-output"
        val clusters = Const.PRJ.TEST_RES.clusters
        val coocs = Const.PRJ.TEST_RES.coocs
        val trigrams = Const.PRJ.TEST_RES.trigrams
        val deps = Const.PRJ.TEST_RES.deps
        val contexts = Const.PRJ.TEST_RES.contexts


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
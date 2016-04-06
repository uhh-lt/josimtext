import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._
class SenseFeatureAggregatorTest extends FlatSpec with ShouldMatchers {
    "The SenseFeatureAggregatorTest object" should "skip wrong trigrams" in {
        SenseFeatureAggregator.keepFeature("programming_@_22", "trigrams") should equal(false)
        SenseFeatureAggregator.keepFeature("programming_@_.[22][23]", "trigrams") should equal(false)
        SenseFeatureAggregator.keepFeature("[27]_@_philosophy", "trigrams") should equal(false)
    }


    def agg() = {
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

        //SenseFeatureAggregator.run(sc, sensesPath, wordsPath, featuresPath, wordFeaturesPath, outputPath)
    }

//    "Aggregate WordNet" should "run" in {
//        agg(WSDFeatures.Clusters)
//    }


}

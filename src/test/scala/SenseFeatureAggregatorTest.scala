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
        val words = Const.PRJ.TEST_RES.wordsTrigram
        val features = Const.PRJ.TEST_RES.featuresTrigram
        val wordFeatures = Const.PRJ.TEST_RES.wordsFeaturesTrigram
        val featureType = "trigrams"
        println(s"Senses: $senses")
        println(s"Output: $output")

        val conf = new SparkConf()
            .setAppName("JST: WSD")
            .setMaster("local[1]")
        val sc = new SparkContext(conf)

        SenseFeatureAggregator.run(sc, senses, words, features, wordFeatures, output, featureType)
    }

    "Aggregate PRJ" should "run" in {
        agg()
    }
}

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import utils.Const
import wsd.SenseFeatureAggregator

class SenseFeatureAggregatorTest extends FlatSpec with Matchers {
    "The SenseFeatureAggregatorTest object" should "skip wrong trigrams" in {
        SenseFeatureAggregator.keepFeature("programming_@_22", "trigrams") should equal(false)
        SenseFeatureAggregator.keepFeature("programming_@_.[22][23]", "trigrams") should equal(false)
        SenseFeatureAggregator.keepFeature("[27]_@_philosophy", "trigrams") should equal(false)
    }

    def agg(senses:String) = {
        val output =  senses + "-output"
        val words = Const.PRJ_TEST.WSD_RES.wordsTrigram
        val features = Const.PRJ_TEST.WSD_RES.featuresTrigram
        val wordFeatures = Const.PRJ_TEST.WSD_RES.wordsFeaturesTrigram
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
        val senses =  getClass.getResource(Const.PRJ_TEST.SENSES).getPath()
        agg(senses)
    }

    "Aggregate WordNet" should "run" in {
        val senses =  "/Users/alex/Desktop/topic_signatures_kb_semeval_16/inventory/senses.csv"
        agg(senses)
    }
}

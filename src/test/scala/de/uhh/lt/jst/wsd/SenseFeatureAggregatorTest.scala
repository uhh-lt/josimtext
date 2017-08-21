package de.uhh.lt.jst.wsd

import com.holdenkarau.spark.testing.SharedSparkContext
import de.uhh.lt.testtags.NeedsMissingFiles
import org.scalatest._
import de.uhh.lt.jst.utils.Const

class SenseFeatureAggregatorTest extends FlatSpec with Matchers with SharedSparkContext {
  "The SenseFeatureAggregatorTest object" should "skip wrong trigrams" in {
    SenseFeatureAggregator.keepFeature("programming_@_22", "trigrams") should equal(false)
    SenseFeatureAggregator.keepFeature("programming_@_.[22][23]", "trigrams") should equal(false)
    SenseFeatureAggregator.keepFeature("[27]_@_philosophy", "trigrams") should equal(false)
  }

  def agg(senses: String) = {
    val output = senses + "-output"
    val words = Const.PRJ_TEST.WSD_RES.wordsTrigram
    val features = Const.PRJ_TEST.WSD_RES.featuresTrigram
    val wordFeatures = Const.PRJ_TEST.WSD_RES.wordsFeaturesTrigram
    val featureType = "trigrams"
    println(s"Senses: $senses")
    println(s"Output: $output")

    SenseFeatureAggregator.run(sc, senses, words, features, wordFeatures, output, featureType)
  }

  ignore should "run Aggregate PRJ" taggedAs NeedsMissingFiles in {
    val senses = getClass.getResource(Const.PRJ_TEST.SENSES).getPath()
    agg(senses)
  }

  ignore should "run Aggregate WordNet" taggedAs NeedsMissingFiles in {
    val senses = "/Users/alex/Desktop/topic_signatures_kb_semeval_16/inventory/senses.csv"
    agg(senses)
  }
}

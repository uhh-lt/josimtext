package de.uhh.lt.jst.wsd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import de.uhh.lt.jst.utils.Const
import de.uhh.lt.testtags.NeedsMissingFiles

class SenseFeatureAggregatorTest extends FlatSpec with Matchers with SharedSparkContext {
  "The SenseFeatureAggregatorTest object" should "skip wrong trigrams" in {
    SenseFeatureAggregator.keepFeature("programming_@_22", "trigrams") should equal(false)
    SenseFeatureAggregator.keepFeature("programming_@_.[22][23]", "trigrams") should equal(false)
    SenseFeatureAggregator.keepFeature("[27]_@_philosophy", "trigrams") should equal(false)
  }

  def agg(senses: String) = {
    val output = senses + "-output"
    val words = Const.PRJ_TEST.WORDS
    val features = Const.PRJ_TEST.FEATURES
    val wordFeatures = Const.PRJ_TEST.WORD_FEATURES
    val featureType = "trigrams"
    println(s"Senses: $senses")
    println(s"Output: $output")
    SenseFeatureAggregator.run(sc, senses, words, features, wordFeatures, output, featureType)
  }

  ignore should "run Aggregate PRJ" taggedAs NeedsMissingFiles in {
    val senses = getClass.getResource(Const.PRJ_TEST.SENSES).getPath()
    agg(senses)
  }
}

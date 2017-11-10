package de.uhh.lt.jst.wsd

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest._
import de.uhh.lt.jst.utils.Const
import de.uhh.lt.testing.tags.NeedsMissingFiles
import SenseFeatureAggregator.FeatureType.trigrams
import SenseFeatureAggregator.Config

class SenseFeatureAggregatorTest extends FlatSpec with Matchers with DatasetSuiteBase {
  "The SenseFeatureAggregatorTest object" should "skip wrong trigrams" in {
    SenseFeatureAggregator.keepFeature("programming_@_22", trigrams) should equal(false)
    SenseFeatureAggregator.keepFeature("programming_@_.[22][23]", trigrams) should equal(false)
    SenseFeatureAggregator.keepFeature("[27]_@_philosophy", trigrams) should equal(false)
  }

  def agg(senses: String): Unit = {
    val config = Config(
      sensesPath = senses,
      wordsPath = Const.PRJ_TEST.WORDS,
      featuresPath = Const.PRJ_TEST.FEATURES,
      wordFeaturesPath = Const.PRJ_TEST.WORD_FEATURES,
      outputPath = senses + "-output",
      featureType = trigrams
    )
    SenseFeatureAggregator.run(spark, config)
  }

  ignore should "run Aggregate PRJ" taggedAs NeedsMissingFiles in {
    val senses = getClass.getResource(Const.PRJ_TEST.SENSES).getPath()
    agg(senses)
  }
}

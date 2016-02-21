import org.scalatest._
class SenseFeatureAggregatorTest extends FlatSpec with ShouldMatchers {
    "The SenseFeatureAggregatorTest object" should "skip wrong trigrams" in {
        SenseFeatureAggregator.keepFeature("programming_@_22", "trigrams") should equal(false)
        SenseFeatureAggregator.keepFeature("programming_@_.[22][23]", "trigrams") should equal(false)
        SenseFeatureAggregator.keepFeature("[27]_@_philosophy", "trigrams") should equal(false)
    }
}

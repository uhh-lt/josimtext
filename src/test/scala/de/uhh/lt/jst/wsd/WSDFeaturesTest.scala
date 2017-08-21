package de.uhh.lt.jst.wsd

import org.scalatest._

class WSDFeaturesTest extends FlatSpec with Matchers {
  "The WSDFeatureTest enumerateion" should "detect modes that require all but trigrams" in {
    val m = WSDFeatures.DepsallCoocsClusters
    WSDFeatures.depstargetNeeded(m) should equal(true)
    WSDFeatures.depsallNeeded(m) should equal(true)
    WSDFeatures.clustersNeeded(m) should equal(true)
    WSDFeatures.coocsNeeded(m) should equal(true)
    WSDFeatures.trigramsNeeded(m) should equal(false)
  }


  "The WSDFeatureTest enumerateion" should "detect modes that require all features" in {
    val m = WSDFeatures.DepsallCoocsClustersTrigramsall
    WSDFeatures.depstargetNeeded(m) should equal(true)
    WSDFeatures.depsallNeeded(m) should equal(true)
    WSDFeatures.clustersNeeded(m) should equal(true)
    WSDFeatures.coocsNeeded(m) should equal(true)
    WSDFeatures.trigramsNeeded(m) should equal(true)
  }


  "The WSDFeatureTest enumerateion" should "detect modes that require trigrams" in {
    val m = WSDFeatures.Trigramsall
    WSDFeatures.trigramsNeeded(m) should equal(true)
    WSDFeatures.trigramstargetNeeded(m) should equal(true)
    WSDFeatures.trigramsallNeeded(m) should equal(true)
    WSDFeatures.depsallNeeded(m) should equal(false)
    WSDFeatures.clustersNeeded(m) should equal(false)
    WSDFeatures.coocsNeeded(m) should equal(false)
  }

}

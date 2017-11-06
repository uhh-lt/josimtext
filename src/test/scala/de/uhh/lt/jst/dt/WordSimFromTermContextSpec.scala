package de.uhh.lt.jst.dt

import com.holdenkarau.spark.testing.{RDDComparisons, DatasetSuiteBase}
import org.scalatest._


class WordSimFromTermContextSpec extends FlatSpec with Matchers with DatasetSuiteBase with RDDComparisons {

  it should "calculate count RDDs" in {

    val path = Option(getClass.getResource("/term-context-pairs.csv")).map(_.getPath).get
    val (wordFeatureCountsRDD, wordCountsRDD, featureCountsRDD) =
      WordSimFromTermContext.calculateCountRDDsFromTextContext(sc, path)

    val expectedWordFeatureCounts = sc.parallelize(Seq(
      ("term1", ("context1", 2)),
      ("term2", ("context1", 1)),
      ("term3", ("context1", 1)),
      ("term1", ("context2", 1)),
      ("term2", ("context2", 1))
    ))
    assert(None === compareRDD(wordFeatureCountsRDD, wordFeatureCountsRDD))

    val expectedWordCounts = sc.parallelize(Seq(
      ("term1", 3),
      ("term2", 2),
      ("term3", 1)
    ))
    assert(None === compareRDD(expectedWordCounts, wordCountsRDD))

    val expectedFeatureCounts = sc.parallelize(Seq(
      ("context1", 4),
      ("context2", 2)
    ))
    assert(None === compareRDD(expectedWordCounts, wordCountsRDD))
  }
}

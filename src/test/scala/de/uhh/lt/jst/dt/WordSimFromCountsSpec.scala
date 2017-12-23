package de.uhh.lt.jst.dt

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, Utils}
import de.uhh.lt.jst.dt.WordSimLib.{TermCountRDD, TermTermCountRDD, TermTermScoresRDD, WordSimParameters}
import org.apache.spark.sql.DataFrame
import org.scalatest._


/**
  * Method call graph [with side effects] for system under test (SUT):
  *
  * WordSimFromCounts.main [reads file]
  * +- WordSimLib.computeWordSimsWithFeatures [writes file]
  *    +- WordSimLib.computeFeatureScores
  *
  */
class WordSimFromCountsSpec extends FlatSpec with Matchers with DataFrameSuiteBase with RDDComparisons {

  val params = WordSimParameters(
    wordsPersFeature = 1000, // val wordsPerFeatureNum: Int = 1000
    minSignificance = 0.0,  // val significanceMin: Double = 0.0
    minWordFeatureCount = 1, // val wordFeatureMinCount: Int = 1
    minWordCount = 1, // val wordMinCount: Int = 1
    minFeatureCount = 1, // val featureMinCount: Int = 1
    significanceType = "LMI", // val significanceType: String = "LMI"
    featuresPerWord = 2000, // val featuresPerWordNum: Int = 2000
    maxSimilarWords = 200 // val similarWordsMaxNum: Int = 200
  )

  val outputDir: String = Utils.createTempDir().getCanonicalPath

  it should "test WordSimLib.computeFeatureScores" in {

    val inputDir = Option(this.getClass.getResource("/prj/counts")).map(_.getPath).get
    val (wordFeatureCountsRDD, wordCountsRDD, featureCountsRDD) = readCountsRDD(inputDir)

    val resultRDD: TermTermScoresRDD =
      WordSimLib.computeFeatureScores(
        wordFeatureCountsRDD,
        wordCountsRDD,
        featureCountsRDD,
        outputDir,
        params
      )

      import spark.implicits._
      val expectedRDD: DataFrame = spark.sparkContext.parallelize(

        Seq(
          ("2.3[edit", Array(("Ruby_@_]",11.847281106470364))),
          ("IDLE", Array(("include_@_and",12.654636028527968))),
          ("someone", Array(
            (",_@_else",13.069673527806811),
            ("#_@_may",10.747745432919448),
            ("to_@_,",9.262318605749208))
          ),
          ("bone",Array(
            ("heaviest_@_\"",13.654636028527968),
            ("temporal_@_of",13.654636028527968)
          )),
          ("nobleman", Array(("of_@_in",11.654636028527968)))
        )
    ).toDF

    assertDataFrameApproximateEquals(expectedRDD, resultRDD.toDF.limit(5), 0.01)
  }

  it should "test computeWordSimsWithFeatures" in {

    val inputDir = Option(getClass.getResource("/prj/counts")).map(_.getPath).get
    val (wordFeatureCountsRDD, wordCountsRDD, featureCountsRDD) = readCountsRDD(inputDir)

    val (simsPath, simsWithFeaturesPath, featuresPath) =
      WordSimLib.computeWordSimsWithFeatures(
        wordFeatureCountsRDD,
        wordCountsRDD,
        featureCountsRDD,
        outputDir,
        params
      )

    val expectedFolder = Option(getClass.getResource("/prj/dt")).map(_.getPath).get

    val expectedWordSimsRDD = sc.textFile(s"$expectedFolder/SimPruned")
    val resultWordSimsRDD = sc.textFile(s"$outputDir/SimPruned")

    val expectedFeaturesPerWordRDD = sc.textFile(s"$expectedFolder/FeaturesPruned")
    val resultFeaturesPerWordRDD = sc.textFile(s"$outputDir/FeaturesPruned")

    assert(None === compareRDD(expectedWordSimsRDD, resultWordSimsRDD))
    assert(None === compareRDD(expectedFeaturesPerWordRDD, resultFeaturesPerWordRDD))
  }

  private def readCountsRDD(folder: String): (TermTermCountRDD, TermCountRDD, TermCountRDD) = {

    val wordFeatureCountsRDD = sc.textFile(s"$folder/WF.csv")
      .map(line => line.split("\t"))
      .map {
        case Array(word, feature, count) => (word, (feature, count.toInt))
        case _ => ("?", ("?", 0))
      }

    val wordCountsRDD = sc.textFile(s"$folder/W.csv")
      .map(line => line.split("\t"))
      .map {
        case Array(word, count) => (word, count.toInt)
        case _ => ("?", 0)
      }

    val featureCountsRDD = sc.textFile(s"$folder/F.csv")
      .map(line => line.split("\t"))
      .map {
        case Array(feature, count) => (feature, count.toInt)
        case _ => ("?", 0)
      }


    (wordFeatureCountsRDD, wordCountsRDD, featureCountsRDD)

  }

}

package de.uhh.lt.jst.dt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordSimFromTermContext {
  val wordsPerFeatureNumDefault = 1000
  val significanceMinDefault = 0.0
  val wordFeatureMinCountDefault = 2
  val wordMinCountDefault = 2
  val featureMinCountDefault = 2
  val significanceTypeDefault = "LMI"
  val featuresPerWordNumDefault = 1000
  val similarWordsMaxNumDefault = 200


  def main(args: Array[String]) {
    if (args.size < 2) {
      println("Usage: term-context-pairs output-dir [parameters]")
      println("parameters: wordsPerFeatureNum featuresPerWordNum wordMinCount featureMinCount wordFeatureMinCount significanceMin significanceType similarWordsMaxNum")
      return
    }

    val termContextPath = args(0)
    val outputDir = args(1)
    val wordsPerFeatureNum = if (args.size > 2) args(2).toInt else wordsPerFeatureNumDefault
    val featuresPerWordNum = if (args.size > 3) args(3).toInt else featuresPerWordNumDefault
    val wordMinCount = if (args.size > 4) args(4).toInt else wordMinCountDefault
    val featureMinCount = if (args.size > 5) args(5).toInt else featureMinCountDefault
    val wordFeatureMinCount = if (args.size > 6) args(6).toInt else wordFeatureMinCountDefault
    val significanceMin = if (args.size > 7) args(7).toDouble else significanceMinDefault
    val significanceType = if (args.size > 8) args(8) else significanceTypeDefault
    val similarWordsMaxNum = if (args.size > 9) args(9).toInt else similarWordsMaxNumDefault

    val conf = new SparkConf().setAppName("JST: WordSimFromTermContext")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    run(sc, termContextPath, outputDir, wordsPerFeatureNum, featuresPerWordNum, wordMinCount, featureMinCount, wordFeatureMinCount, significanceMin, significanceType, similarWordsMaxNum)
  }

  type TermTermCountRDD = RDD[(String, (String, Int))]
  type TermCountRDD =  RDD[(String, Int)]
  type TermTermScoresRDD = RDD[(String, Array[(String, Double)])]

  def calculateCountRDDsFromTextContext(sc: SparkContext, path: String): (
    TermTermCountRDD, TermCountRDD, TermCountRDD) = {

    val termContextRDD = sc.textFile(path)
      .map(line => line.split("\t"))
      .map{ case Array(term, context) => (term, context)}


    val termFeatureCountRDD = termContextRDD // Note: feature = context
      .map { case (term, context) => ((term, context), 1) }
      .reduceByKey(_ + _)
      .map { case ((term, context), count) => (term, (context, count))}

    val termCountRDD = termContextRDD
      .map { case (term, context) => (term, 1) }
      .reduceByKey(_ + _)

    val featureCountRDD = termContextRDD
      .map { case (term, context) => (context, 1) }
      .reduceByKey(_ + _)

    (termFeatureCountRDD, termCountRDD, featureCountRDD)
  }

  def run(sc: SparkContext,
          termContextPath: String,
          outputDir: String,
          wordsPerFeatureNum: Int = wordsPerFeatureNumDefault,
          featuresPerWordNum: Int = featuresPerWordNumDefault,
          wordMinCount: Int = wordMinCountDefault,
          featureMinCount: Int = featureMinCountDefault,
          wordFeatureMinCount: Int = wordFeatureMinCountDefault,
          significanceMin: Double = significanceMinDefault,
          significanceType: String = significanceTypeDefault,
          similarWordsMaxNum: Int = similarWordsMaxNumDefault) = {

    val (wordFeatureCounts, wordCounts, featureCounts) = calculateCountRDDsFromTextContext(sc, termContextPath)

    val (simsPath, simsWithFeaturesPath, featuresPath) = WordSimLib.computeWordSimsWithFeatures(wordFeatureCounts, wordCounts, featureCounts, outputDir, wordsPerFeatureNum, featuresPerWordNum, wordMinCount, featureMinCount, wordFeatureMinCount, significanceMin, similarWordsMaxNum, significanceType)

    println(s"Word similarities: $simsPath")
    println(s"Word similarities with features: $simsWithFeaturesPath")
    println(s"Features: $featuresPath")
  }
}

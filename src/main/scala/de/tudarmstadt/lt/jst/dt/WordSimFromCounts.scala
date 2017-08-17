package de.tudarmstadt.lt.jst.dt

import org.apache.spark.{SparkConf, SparkContext}

object WordSimFromCounts {
    val wordsPerFeatureNumDefault = 1000
    val significanceMinDefault = 0.0
    val wordFeatureMinCountDefault = 2
    val wordMinCountDefault = 2
    val featureMinCountDefault = 2
    val significanceTypeDefault = "LMI"
    val featuresPerWordNumDefault = 1000
    val similarWordsMaxNumDefault = 200

    def main(args: Array[String]) {
        if (args.size < 4) {
            println("Usage: word-counts feature-counts word-feature-counts output-dir [parameters]")
            println("parameters: wordsPerFeatureNum featuresPerWordNum wordMinCount featureMinCount wordFeatureMinCount significanceMin significanceType similarWordsMaxNum")
            return
        }

        val wordCountsPath = args(0)
        val featureCountsPath = args(1)
        val wordFeatureCountsPath = args(2)
        val outputDir = args(3)
        val wordsPerFeatureNum = if (args.size > 4) args(4).toInt else wordsPerFeatureNumDefault
        val featuresPerWordNum = if (args.size > 5) args(5).toInt else featuresPerWordNumDefault
        val wordMinCount = if (args.size > 6) args(6).toInt else wordMinCountDefault
        val featureMinCount = if (args.size > 7) args(7).toInt else featureMinCountDefault
        val wordFeatureMinCount = if (args.size > 8) args(8).toInt else wordFeatureMinCountDefault
        val significanceMin = if (args.size > 9) args(9).toDouble else significanceMinDefault
        val significanceType = if (args.size > 10) args(10) else significanceTypeDefault
        val similarWordsMaxNum = if (args.size > 11) args(11).toInt else similarWordsMaxNumDefault

        val conf = new SparkConf().setAppName("JST: WordSimFromCounts")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        run(sc, wordCountsPath, featureCountsPath, wordFeatureCountsPath, outputDir, wordsPerFeatureNum, featuresPerWordNum, wordMinCount, featureMinCount, wordFeatureMinCount, significanceMin, significanceType, similarWordsMaxNum)
    }

    def run(sc: SparkContext,
            wordCountsPath: String,
            featureCountsPath: String,
            wordFeatureCountsPath: String,
            outputDir: String,
            wordsPerFeatureNum: Int = wordsPerFeatureNumDefault,
            featuresPerWordNum: Int = featuresPerWordNumDefault,
            wordMinCount: Int = wordMinCountDefault,
            featureMinCount: Int = featureMinCountDefault,
            wordFeatureMinCount: Int = wordFeatureMinCountDefault,
            significanceMin: Double = significanceMinDefault,
            significanceType: String = significanceTypeDefault,
            similarWordsMaxNum: Int = similarWordsMaxNumDefault) = {

        val wordFeatureCounts = sc.textFile(wordFeatureCountsPath)
            .map(line => line.split("\t"))
            .map{
                case Array(word, feature, count) => (word, (feature, count.toInt))
                case _ => ("?", ("?", 0))
            }

        val wordCounts = sc.textFile(wordCountsPath)
            .map(line => line.split("\t"))
            .map{
                case Array(word, count) => (word, count.toInt)
                case _ => ("?", 0)
            }

        val featureCounts = sc.textFile(featureCountsPath)
            .map(line => line.split("\t"))
            .map{
                case Array(feature, count) => (feature, count.toInt)
                case _ => ("?", 0)
            }

        val (simsPath, simsWithFeaturesPath, featuresPath) = WordSimLib.computeWordSimsWithFeatures(wordFeatureCounts, wordCounts, featureCounts, outputDir, wordsPerFeatureNum, featuresPerWordNum, wordMinCount, featureMinCount, wordFeatureMinCount, significanceMin, similarWordsMaxNum, significanceType)

        println(s"Word similarities: $simsPath")
        println(s"Word similarities with features: $simsWithFeaturesPath")
        println(s"Features: $featuresPath")
    }
}

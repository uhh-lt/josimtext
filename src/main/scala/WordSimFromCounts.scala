import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordSimFromCounts {
    val wordsPerFeatureNumDefault = 1000
    val significanceMinDefault = 0.0
    val wordFeatureMinCountDefault = 2
    val wordMinCountDefault = 2
    val featureMinCountDefault = 2
    val significanceTypeDefault = "LMI"
    val simScoreDigitsNumDefault = 5
    val featuresPerWordNumDefault = 1000
    val similarWordsMaxNumDefault = 200

    def main(args: Array[String]) {
        if (args.size < 1) {
            println("Usage: WordSim word-feature-counts word-counts feature-counts output [w=10000] [s=0.0] [t_wf=2] [t_w=2] [t_f=2] [sig=LMI] [r=3] [p=1000] [l=200]")
            println("For example, the arguments \"wikipedia wikipedia-out 100000 0.0 3\" will override w with 100000 and t_wf with 3, leaving the rest at the default values")
            return
        }

        val wordFeatureCountsPath = args(0)
        val wordCountsPath = args(1)
        val featureCountsPath = args(2)
        val outputDir = args(3)
        val wordsPerFeatureNum = if (args.size > 4) args(4).toInt else wordsPerFeatureNumDefault
        val significanceMin = if (args.size > 5) args(5).toDouble else significanceMinDefault
        val wordFeatureMinCount = if (args.size > 6) args(6).toInt else wordFeatureMinCountDefault
        val wordMinCount = if (args.size > 7) args(7).toInt else wordMinCountDefault
        val featureMinCount = if (args.size > 8) args(8).toInt else featureMinCountDefault
        val significanceType = if (args.size > 9) args(9) else significanceTypeDefault
        val simScoreDigitsNum = if (args.size > 10) args(10).toInt else simScoreDigitsNumDefault
        val featuresPerWordNum = if (args.size > 11) args(11).toInt else featuresPerWordNumDefault
        val similarWordsMaxNum = if (args.size > 12) args(12).toInt else similarWordsMaxNumDefault

        val conf = new SparkConf().setAppName("JST: WordSimFromCounts")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        run(sc, wordFeatureCountsPath, wordCountsPath, featureCountsPath, outputDir,
            wordsPerFeatureNum, significanceMin, wordFeatureMinCount, wordMinCount,
            featureMinCount, significanceType, simScoreDigitsNum, featuresPerWordNum, similarWordsMaxNum)
    }

    def run(sc: SparkContext, wordFeatureCountsPath: String, wordCountsPath: String, featureCountsPath: String, outputDir: String,
            wordsPerFeatureNum: Int=wordsPerFeatureNumDefault,
            significanceMin: Double=significanceMinDefault,
            wordFeatureMinCount: Int=wordFeatureMinCountDefault,
            wordMinCount: Int=wordMinCountDefault,
            featureMinCount: Int=featureMinCountDefault,
            significanceType: String=significanceTypeDefault,
            simScoreDigitsNum: Int=simScoreDigitsNumDefault,
            featuresPerWordNum: Int=featuresPerWordNumDefault,
            similarWordsMaxNum:Int=similarWordsMaxNumDefault) = {

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

        val (simsPath, simsWithFeaturesPath, featuresPath) = WordSimLib.computeWordSimsWithFeatures(
            wordFeatureCounts, wordCounts, featureCounts, wordsPerFeatureNum,
            wordFeatureMinCount, wordMinCount, featureMinCount, significanceMin,
            featuresPerWordNum, similarWordsMaxNum, significanceType, simScoreDigitsNum, outputDir)

        println(s"Word similarities: $simsPath")
        println(s"Word similarities with features: $simsWithFeaturesPath")
        println(s"Features: $featuresPath")
    }
}

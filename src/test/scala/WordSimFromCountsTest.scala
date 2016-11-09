import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._


class WordSimFromCountsTest extends FlatSpec with ShouldMatchers {

    def run(outputPath:String) = {
        val words = getClass.getResource(Const.PRJ.WORDS).getPath()
        val features = getClass.getResource(Const.PRJ.FEATURES).getPath()
        val wordFeatures = getClass.getResource(Const.PRJ.WORD_FEATURES).getPath()

        println(s"Output: $outputPath")

        val conf = new SparkConf()
            .setAppName("JST: WordSimFromCounts test")
            .setMaster("local[1]")
        val sc = new SparkContext(conf)

        Util.delete(outputPath)
        WordSimFromCounts.run(sc, wordFeatures, words, features, outputPath,
            significanceMin=0.0,
            wordFeatureMinCount=1,
            wordMinCount=1,
            featureMinCount=1);
    }

    "DefaultConf" should "produce default results" in {
        run("output")
        // parse files in the output and check their contents
    }

}

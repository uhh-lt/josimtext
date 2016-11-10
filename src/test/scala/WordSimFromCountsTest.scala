import java.io.File
import java.nio.file.{FileSystems, Files, Paths}
import Util.gzip
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import scala.io.Source


class WordSimFromCountsTest extends FlatSpec with ShouldMatchers {

    def run() = {
        val words = getClass.getResource(Const.PRJ.WORDS).getPath()
        val features = getClass.getResource(Const.PRJ.FEATURES).getPath()
        val wordFeatures = getClass.getResource(Const.PRJ.WORD_FEATURES).getPath()
        val outputDir = FileSystems.getDefault().getPath(new File(".").getCanonicalPath()) + "/output";
        Util.delete(outputDir)
        println(s"Output: $outputDir")

        val conf = new SparkConf()
            .setAppName("JST: WordSimFromCounts test")
            .setMaster("local[1]")
        val sc = new SparkContext(conf)

        WordSimFromCounts.run(sc, wordFeatures, words, features, outputDir,
            significanceMin=0.0,
            wordFeatureMinCount=1,
            wordMinCount=1,
            featureMinCount=1)
    }

    "DefaultConf" should "produce default results" in {
        run()
        // parse files in the output and check their contents
    }

    "DefaultConfParse" should "produce default results" in {
        val outputDir = FileSystems.getDefault().getPath(new File(".").getCanonicalPath()) + "/output";

        // Schould have output directories
        val simPath = Paths.get(outputDir, "SimPruned");
        Files.exists(simPath) should equal(true)

        val simFeaturesPath = Paths.get(outputDir, "SimPrunedWithFeatures");
        Files.exists(simFeaturesPath) should equal(true)

//        val featuresPath = Paths.get(outputDir, "FeaturesPruned");
//        Files.exists(featuresPath) should equal(true)

        // Should contain files with data
        Files.exists(Paths.get(simPath.toString, "part-00123.gz")) should equal(true)

        val pythonSimPath = Paths.get(simPath.toString, "part-00908.gz");
        Files.exists(pythonSimPath) should equal(true)

        // Number of lines in the files should be as expected
        val pythonLines = Source.fromInputStream(gzip(pythonSimPath.toString)).getLines.toList
        pythonLines.size should equal(313)

        // Content of the SimPruned
        pythonLines should contain("python\tPyPy\t0.001")
        pythonLines should not contain("python\tPyPy\t0.002")
        pythonLines should contain("python\tcpython\t0.001")

        // Content of FeaturesPruned

    }

}

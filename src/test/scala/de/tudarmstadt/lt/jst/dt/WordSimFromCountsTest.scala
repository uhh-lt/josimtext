package de.tudarmstadt.lt.jst.dt

import java.io.File
import java.nio.file.{FileSystems, Files, Paths}

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import org.scalatest.tagobjects.Slow
import utils.Util.gzip
import utils.{Const, Util}

import scala.io.Source


class WordSimFromCountsTest extends FlatSpec with Matchers with SharedSparkContext {

  /**
    * This function and thus each test case run about about 2 minutes on a core i5 cpu with 8gb of ram
    **/
  def run() = {
    val words = getClass.getResource(Const.PRJ_TEST.WORDS).getPath()
    val features = getClass.getResource(Const.PRJ_TEST.FEATURES).getPath()
    val wordFeatures = getClass.getResource(Const.PRJ_TEST.WORD_FEATURES).getPath()
    val outputDir = FileSystems.getDefault().getPath(new File(".").getCanonicalPath()) + "/output";
    Util.delete(outputDir)
    println(s"Output: $outputDir")

    WordSimFromCounts.run(sc, words, features, wordFeatures, outputDir, wordMinCount = 1, featureMinCount = 1, wordFeatureMinCount = 1, significanceMin = 0.0)
  }

  ignore should "DefaultConf produce default results" taggedAs Slow in {
    run()

    val outputDir = FileSystems.getDefault().getPath(new File(".").getCanonicalPath()) + "/output";

    /////////////////////
    // Check similariries

    // Should have output directories
    val simPath = Paths.get(outputDir, "SimPruned");
    Files.exists(simPath) should equal(true)

    // Should contain files with data
    Files.exists(Paths.get(simPath.toString, "part-00123.gz")) should equal(true)
    val pythonSimPath = Paths.get(simPath.toString, "part-00908.gz");
    Files.exists(pythonSimPath) should equal(true)

    // Number of lines in the files should be as expected
    val pythonLines = Source.fromInputStream(gzip(pythonSimPath.toString)).getLines.toList
    pythonLines.size should equal(313)

    // Content of the SimPruned
    pythonLines should contain("python\tPyPy\t0.00100")
    pythonLines should not contain ("python\tPyPy\t0.00200")
    pythonLines should contain("python\tcpython\t0.00100")

    /////////////////////
    // Check features

    // Schould have output directories
    val featuresDirPath = Paths.get(outputDir, "FeaturesPruned");
    Files.exists(featuresDirPath) should equal(true)

    // Should contain files with data
    val featuresPath = Paths.get(featuresDirPath.toString, "part-00000.gz");
    Files.exists(featuresPath) should equal(true)

    // Number of lines in the files should be as expected
    val featuresLines = Source.fromInputStream(gzip(featuresPath.toString)).getLines.toList
    featuresLines.size should equal(22983)

    // Content of the SimPruned
    featuresLines should contain("python\tthe_@_community\t28.96391")
    featuresLines should contain("python\tdesktop_@_ides\t9.65464")
    featuresLines should contain("Ruby\tthe_@_License\t16.59417")
  }
}

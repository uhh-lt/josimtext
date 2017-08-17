package de.tudarmstadt.lt.jst.corpus

import java.nio.file.{Files, Paths}

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._

import scala.io.Source

class CoarsifyPosTagsTest extends FlatSpec with Matchers with SharedSparkContext {
  val COUNTS_DIR = getClass.getResource("/counts").getPath

  def run(inputDir: String) = {
    val outputDir = inputDir + "-output"
    CoarsifyPosTags.run(sc, inputDir, outputDir)
    outputDir
  }

  "full2coarse" should "run" in {
    CoarsifyPosTags.full2coarse("VBZ") should equal("VB")
    CoarsifyPosTags.full2coarse("VB") should equal("VB")
    CoarsifyPosTags.full2coarse("VBZ") should equal("VB")
    CoarsifyPosTags.full2coarse("NNP") should equal("NP")

  }

  "CoarsifyPosTags" should "produce expected results" in {
    val outputDir = run(COUNTS_DIR)

    // length of the ouput W and WF files should be 1000 (input is longer!)
    Files.exists(Paths.get(outputDir)) should equal(true)
    Files.exists(Paths.get(outputDir, "W")) should equal(true)
    Files.exists(Paths.get(outputDir, "F")) should equal(true)
    Files.exists(Paths.get(outputDir, "WF")) should equal(true)
    Files.exists(Paths.get(outputDir, "W/part-00000")) should equal(true)
    Files.exists(Paths.get(outputDir, "WF/part-00000")) should equal(true)

    val wLines = Source.fromFile(Paths.get(outputDir, "W/part-00000").toString).getLines.toList
    wLines.length should equal(1000)
    val wfLines = Source.fromFile(Paths.get(outputDir, "WF/part-00000").toString).getLines.toList
    wfLines.length should equal(1000)
  }

  "CoarsifyPosTags.coarsifyPosTag" should "return correct result" in {
    CoarsifyPosTags.coarsifyPosTag("Python#NNS") should equal("Python#NN")
    CoarsifyPosTags.coarsifyPosTag("Python#VBD") should equal("Python#VB")
    CoarsifyPosTags.coarsifyPosTag("Python#NNP") should equal("Python#NP")
    CoarsifyPosTags.coarsifyPosTag("Python#NNPS") should equal("Python#NP")
    CoarsifyPosTags.coarsifyPosTag("Python#CC") should equal("Python#CC")
    CoarsifyPosTags.coarsifyPosTag("Python#ABC") should equal("Python#ABC")
    CoarsifyPosTags.coarsifyPosTag("C##NNP") should equal("C##NP")
    CoarsifyPosTags.coarsifyPosTag("C#2.0#NNP") should equal("C#2.0#NP")
    CoarsifyPosTags.coarsifyPosTag("Python#NNP programming#JJ language#NNS") should equal("Python#NP programming#JJ language#NN")
    CoarsifyPosTags.coarsifyPosTag("C#2.0#NNP programming#JJ language#NNS") should equal("C#2.0#NP programming#JJ language#NN")
  }
}



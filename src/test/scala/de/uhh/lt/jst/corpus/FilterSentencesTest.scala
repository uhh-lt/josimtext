package de.uhh.lt.jst.corpus

import java.nio.file.Paths

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._

import scala.io.Source

class FilterSentencesTest extends FlatSpec with Matchers with SharedSparkContext {
  val inputPath: String = getClass.getResource("/noisy-sentences.txt.gz").getPath
  val outputPath: String =  inputPath + "-output"

  def run(inputPath: String): String = {
    val outputPath = inputPath + "-output"
    val config = FilterSentences.Config(inputPath, outputPath)
    FilterSentences.run(sc, config)
    outputPath
  }

  it should "filter noisy sentences" in {
    val outputPath = run(inputPath)
    val lines = Source.fromFile(Paths.get(outputPath, "/part-00000").toString).getLines.toList
    lines.length should equal(942) // out of 1000
  }
}


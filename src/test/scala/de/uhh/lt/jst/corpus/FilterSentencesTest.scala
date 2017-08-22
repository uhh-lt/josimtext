package de.uhh.lt.jst.corpus

import java.nio.file.Paths

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._

import scala.io.Source

class FilterSentencesTest extends FlatSpec with Matchers with SharedSparkContext {
  val inputPath = getClass.getResource("/noisy-sentences.txt.gz").getPath
  val outputPath =  inputPath + "-output"

  def run(inputPath: String) = {
    val outputPath = inputPath + "-output"
    FilterSentences.run(sc, inputPath, outputPath)
    outputPath
  }

  it should "filter noisy sentences" in {
    val outputPath = run(inputPath)
    val lines = Source.fromFile(Paths.get(outputPath, "/part-00000").toString).getLines.toList
    lines.length should equal(942) // out of 1000
  }
}


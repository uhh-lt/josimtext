package de.uhh.lt.jst.corpus

import com.holdenkarau.spark.testing.SharedSparkContext
import de.uhh.lt.jst.corpus.ReformatConll.Config
import org.scalatest._

class ReformatConllTest extends FlatSpec with Matchers  with SharedSparkContext {
  def run(inputPath:String) = {
    val outputPath = inputPath + "-output"
    val config = Config(inputDir = inputPath, outputDir = outputPath)
    ReformatConll.run(sc, config)
  }

  it should "run reformat standard" in {
    val conllPath = getClass.getResource("/conll-large.csv.gz").getPath()
    run(conllPath)
  }

}

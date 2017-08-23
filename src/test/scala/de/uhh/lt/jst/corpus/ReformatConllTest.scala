package de.uhh.lt.jst.corpus

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._

class ReformatConllTest extends FlatSpec with Matchers  with SharedSparkContext {
  def run(inputPath:String) = {
    val outputPath = inputPath + "-output"
    ReformatConll.run(sc, inputPath, outputPath)
  }

  it should "run reformat standard" in {
    val conllPath = getClass.getResource("/conll-large.csv.gz").getPath()
    run(conllPath)
  }

}

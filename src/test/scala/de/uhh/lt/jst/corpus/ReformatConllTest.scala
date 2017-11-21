package de.uhh.lt.jst.corpus

import com.holdenkarau.spark.testing.DatasetSuiteBase
import de.uhh.lt.jst.corpus.ReformatConll.Config
import org.scalatest._

class ReformatConllTest extends FlatSpec with Matchers  with DatasetSuiteBase {



  def run(inputPath:String) = {
    val outputPath = inputPath + "-output"
    val config = Config(inputDir = inputPath, outputDir = outputPath)
    ReformatConll.run(spark, config)
  }

  it should "run reformat standard" in {
    val conllPath = getClass.getResource("/conll-large.csv.gz").getPath()
    run(conllPath)
  }

}

package de.uhh.lt.jst.index

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.FunSuite
import de.uhh.lt.jst.utils.Const
import de.uhh.lt.testing.spark.SparkESConfigProvider

import de.uhh.lt.jst.index.MakeUniqCoNLL.Config

class MakeUniqCoNLLTest extends FunSuite with DatasetSuiteBase with SparkESConfigProvider {

  test("index a small conll file") {
    val inputPath = getClass.getResource("/conll-1000-tokens.csv.gz").getPath
    val conf = Config(
      inputDir = inputPath,
      outputDir = inputPath + "-output"
    )

    MakeUniqCoNLL.run(spark, conf)
  }

  ignore("index a large conll file") {
    val inputPath = Const.CoNLL.largeConllPath

    val conf = Config(
      inputDir = inputPath,
      outputDir = inputPath + "-output"
    )

    MakeUniqCoNLL.run(spark, conf)
  }

  ignore("index a very large conll file") {
    val inputPath = Const.CoNLL.xlargeConllPath

    val conf = Config(
      inputDir = inputPath,
      outputDir = inputPath + "-output"
    )

    MakeUniqCoNLL.run(spark, conf)
  }
}

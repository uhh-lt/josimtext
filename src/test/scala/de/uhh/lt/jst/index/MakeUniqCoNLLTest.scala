package de.uhh.lt.jst.index

import org.scalatest.FunSuite
import de.uhh.lt.jst.utils.Const
import org.apache.spark.sql.SparkSession

class MakeUniqCoNLLTest extends FunSuite {

  def run(inputConllPath: String) = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val outputConllPath = inputConllPath + "-output"

    val conf = new MakeUniqCoNLL.Config(
      inputDir = inputConllPath,
      outputDir = outputConllPath)

    MakeUniqCoNLL.run(spark, conf)
  }

  test("index a small conll file") {
    val inputPath = getClass.getResource("/conll-1000-tokens.csv.gz").getPath
    run(inputPath)
  }

  ignore("index a large conll file") {
    val inputPath = Const.CoNLL.largeConllPath
    run(inputPath)
  }

  ignore("index a very large conll file") {
    val inputPath = Const.CoNLL.xlargeConllPath
    run(inputPath)
  }
}

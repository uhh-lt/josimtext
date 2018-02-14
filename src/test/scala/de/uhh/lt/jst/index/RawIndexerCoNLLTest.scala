package de.uhh.lt.jst.index

import de.uhh.lt.jst.utils.Const
import RawIndexerCoNLL.Config
import com.holdenkarau.spark.testing.DatasetSuiteBase
import de.uhh.lt.testing.spark.SparkESConfigProvider
import org.scalatest.FunSuite

class RawIndexerCoNLLTest extends FunSuite with DatasetSuiteBase with SparkESConfigProvider {
  // These tests required an instance of elasticsearch running at localhost

  ignore("index a small conll file") {
    val config = Config(
      inputDir = getClass.getResource("/conll-1000-tokens.csv.gz").getPath,
      outputIndex = "test_raw/small"
    )

    RawIndexerCoNLL.run(spark, config)
  }

  ignore("index a large conll file") {
    val config = Config(
      inputDir = Const.CoNLL.largeConllPath,
      outputIndex =  "test_raw/large"
    )

    RawIndexerCoNLL.run(spark, config)
  }

  ignore("index a very large conll file") {
    val config = Config(
      inputDir = Const.CoNLL.xlargeConllPath,
      outputIndex = "test_raw/xlarge"
    )

    RawIndexerCoNLL.run(spark, config)

  }
}

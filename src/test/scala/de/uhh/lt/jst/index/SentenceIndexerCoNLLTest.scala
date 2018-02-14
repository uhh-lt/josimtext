package de.uhh.lt.jst.index

import com.holdenkarau.spark.testing.DatasetSuiteBase
import de.uhh.lt.jst.utils.Const
import org.scalatest.FunSuite
import de.uhh.lt.jst.index.SentenceIndexerCoNLL.Config
import de.uhh.lt.testing.spark.SparkESConfigProvider


class SentenceIndexerCoNLLTest extends FunSuite  with DatasetSuiteBase with SparkESConfigProvider{
  // These tests required an instance of elasticsearch running at localhost

  ignore("index a small conll file") {
    val conf = Config(
      inputDir = getClass.getResource("/conll-1000-tokens.csv.gz").getPath,
      outputIndex = "test_sentences1/small"
    )
    SentenceIndexerCoNLL.run(spark, conf)
  }

  ignore("index a large conll file") {
    val conf = Config(
      inputDir = Const.CoNLL.largeConllPath,
      outputIndex = "test_sentences2/small"
    )
    SentenceIndexerCoNLL.run(spark, conf)
  }

  ignore("index a very large conll file") {

    val conf = Config(
      inputDir = Const.CoNLL.xlargeConllPath,
      outputIndex = "test_sentences3/xlarge"
    )
    SentenceIndexerCoNLL.run(spark, conf)
  }
}


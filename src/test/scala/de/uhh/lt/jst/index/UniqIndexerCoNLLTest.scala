package de.uhh.lt.jst.index

import com.holdenkarau.spark.testing.DatasetSuiteBase
import de.uhh.lt.jst.utils.Const
import de.uhh.lt.testing.spark.SparkESConfigProvider
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class UniqIndexerCoNLLTest extends FunSuite with DatasetSuiteBase with SparkESConfigProvider {

  def run(inputConllPath: String, index: String, node: String) = {

    val conf = new UniqIndexerCoNLL.Config(
      inputDir = inputConllPath,
      outputIndex = index)

    UniqIndexerCoNLL.run(spark, conf)
  }

  ignore("index a small conll file") {
    val conllPath = getClass.getResource("/conll-1000-tokens.csv.gz").getPath
    run(conllPath, "test_uniq1/small", "localhost")
  }

  ignore("index a large conll file") {
    val conllPath = Const.CoNLL.largeConllPath
    run(conllPath, "test_uniq2/large", "localhost")
  }

  ignore("index a very large conll file") {
    val conllPath = Const.CoNLL.xlargeConllPath
    run(conllPath, "test_uniq3/xlarge", "localhost")
  }
}


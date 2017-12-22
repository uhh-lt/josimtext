package de.uhh.lt.jst.index

import de.uhh.lt.jst.utils.Const
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite


class SentenceIndexerCoNLLTest extends FunSuite {
  // These tests required an instance of elasticsearch running at localhost


  def run(inputConllPath: String, index: String, node: String) = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config("es.index.auto.create", "true")
      .config("es.nodes", node)
      .master("local[*]")
      .getOrCreate()

    val conf = new SentenceIndexerCoNLL.Config(
      inputDir = inputConllPath,
      outputIndex = index)

    SentenceIndexerCoNLL.run(spark, conf)
  }

  ignore("index a small conll file") {
    val conllPath = getClass.getResource("/conll-1000-tokens.csv.gz").getPath
    run(conllPath, "test_sentences1/small", "localhost")
  }

  ignore("index a large conll file") {
    val conllPath = Const.CoNLL.largeConllPath
    run(conllPath, "test_sentences2/large", "localhost")
  }

  ignore("index a very large conll file") {
    val conllPath = Const.CoNLL.xlargeConllPath
    run(conllPath, "test_sentences3/xlarge", "localhost")
  }
}


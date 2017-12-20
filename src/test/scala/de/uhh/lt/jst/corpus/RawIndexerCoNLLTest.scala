package de.uhh.lt.jst.corpus
import de.uhh.lt.jst.index.RawIndexerCoNLL
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class RawIndexerCoNLLTest extends FunSuite {
  // These tests required an instance of elasticsearch running at localhost

  def run(inputConllPath: String, index:String, node: String) = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config("es.index.auto.create", "true")
      .config("es.nodes", node)
      .master("local[*]")
      .getOrCreate()

    val conf = new RawIndexerCoNLL.Config(
      inputDir = inputConllPath,
      outputIndex = index,
      esNodeList = node)

    RawIndexerCoNLL.run(spark, conf)
  }

  ignore("index a small conll file") {
    val conllPath = getClass.getResource("/conll-1000-tokens.csv.gz").getPath
    run(conllPath, "test3/small", "localhost")
  }

  ignore("index a large conll file") {
    val conllPath = "/Users/panchenko/Desktop/es-indexing/part-m-19100.gz"
    run(conllPath, "test6/large", "localhost")
  }

  ignore("index a very large conll file") {
    val conllPath = "/Users/panchenko/Desktop/es-indexing/part-m-18080.gz"
    run(conllPath, "test12/sentences", "localhost")
  }

  ignore("index a very large conll file 2 ") {
    val conllPath = "/Users/sasha/Desktop/part-m-10144.gz"
    run(conllPath, "test3/xlarge", "localhost")
  }
}

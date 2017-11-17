package de.uhh.lt.jst.corpus
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class StoreToElasticSearchTest extends FunSuite {
  // These tests required an instance of elasticsearch running at localhost

  def run(inputConllPath: String, index:String, node: String) = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config("es.index.auto.create", "true")
      .config("es.nodes", node)
      .master("local[*]")
      .getOrCreate()

    val conf = new StoreToElasticSearch.Config(
      inputDir = inputConllPath,
      outputIndex = index,
      esNodeList = node)

    StoreToElasticSearch.run(spark, conf)
  }

  test("index a small conll file") {
    val conllPath = getClass.getResource("/conll-1000-tokens.csv.gz").getPath
    run(conllPath, "test3/small", "localhost")
  }

  test("index a large conll file") {
    val conllPath = "/Users/panchenko/Desktop/es-indexing/part-m-19100.gz"
    run(conllPath, "test3/large", "localhost")
  }

  test("index a very large conll file") {
    val conllPath = "/Users/panchenko/Desktop/es-indexing/part-m-18080.gz"
    run(conllPath, "test3/xlarge", "localhost")
  }

}

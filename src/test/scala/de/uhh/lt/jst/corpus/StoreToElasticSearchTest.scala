package de.uhh.lt.jst.corpus
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class StoreToElasticSearchTest extends FunSuite {

  val spark: SparkSession = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .config("es.index.auto.create", "true")
    .config("es.nodes", "localhost")
    .master("local[1]")
    .getOrCreate()

  def run(inputConllPath:String) = {
    val conf = new StoreToElasticSearch.Config(
      inputDir = inputConllPath,
      outputIndex = "depcc_test/test",
      "ltheadnode:9200")
    StoreToElasticSearch.run(spark, conf)
  }

  test("index a small conll file") {
    val conllPath = getClass.getResource("/conll-1000-tokens.csv.gz").getPath
    run(conllPath)
  }
}

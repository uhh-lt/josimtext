import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class ReformatConllTest extends FlatSpec with ShouldMatchers {
  def run(inputPath:String) = {
    val outputPath = inputPath + "-output"
    val conf = new SparkConf()
      .setAppName("test")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    ReformatConll.run(sc, inputPath, outputPath)
  }

  "reformat standard" should "run" in {
    val conllPath = getClass.getResource("/conll_orig").getPath()
    run(conllPath)
  }

  "reformat large" should "run" in {
    //val conllPath = "/Users/sasha/work/active/joint/JoSimText/src/test/resources/conll_large"
    val conllPath = "/Users/panchenko/Desktop/conll"
    run(conllPath)
  }
}

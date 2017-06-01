
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class Conll2FeaturesTest extends FlatSpec with ShouldMatchers {

    def run(inputPath:String) = {
      val outputPath = inputPath + "-output"

      val conf = new SparkConf()
        .setAppName("test")
        .setMaster("local[*]")
      val sc = new SparkContext(conf)
      Conll2Features.run(sc, inputPath, outputPath)
    }

    "Conll2Features" should "run" in {
      //val senses =  getClass.getResource(Const.PRJ.SENSES).getPath()
      run("/Users/panchenko/Desktop/tmp/conll/part-m-00000.gz")
    }
  }
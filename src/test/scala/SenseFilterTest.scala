import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import wsd.SensesFilter

class SenseFilterTest extends FlatSpec with Matchers {

  def run(sensesPath:String, vocPath:String) = {
    val outputPath = sensesPath + "-output"

    val conf = new SparkConf()
      .setAppName("JST: DT filter")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)

    SensesFilter.runCli(sc, sensesPath, vocPath, outputPath, lowercase = true, lowerOrFirstUpper = true)
  }

  "SenseFilter object" should "filter sense clusters by vocabulary" in {
    //val senses =  getClass.getResource(Const.PRJ.SENSES).getPath()
    run(sensesPath="/Users/sasha/Desktop/w2v-jbt-nns/senses-test.csv",
      vocPath="/Users/sasha/Desktop/w2v-jbt-nns/voc.csv")
  }
}
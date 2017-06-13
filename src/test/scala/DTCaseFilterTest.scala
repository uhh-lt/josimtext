import dt.DTCaseFilter
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class DTCaseFilterTest extends FlatSpec with Matchers {

    def run(dtPath:String) = {
        val outputPath =  dtPath + "-output"

        val conf = new SparkConf()
            .setAppName("JST: DT filter")
            .setMaster("local[1]")
        val sc = new SparkContext(conf)

        DTCaseFilter.run(sc, dtPath, outputPath)
    }

    "DTFilter object" should "filter DT by vocabulary" in {
        //val senses =  getClass.getResource(Const.PRJ.SENSES).getPath()
        run(dtPath="/Users/alex/Desktop/w2v-jbt-nns/dt-text.csv")
    }
}


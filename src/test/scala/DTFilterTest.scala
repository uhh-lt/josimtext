import dt.DTFilter
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class DTFilterTest extends FlatSpec with Matchers {

    def run(dtPath:String, vocPath:String) = {
        val outputPath =  dtPath + "-output"

        val conf = new SparkConf()
            .setAppName("JST: DT filter")
            .setMaster("local[1]")
        val sc = new SparkContext(conf)

        DTFilter.run(sc, dtPath, vocPath, outputPath, keepSingleWords=false, filterOnlyTarget=true)
    }

    "DTFilter object" should "filter DT by vocabulary" in {
        //val senses =  getClass.getResource(Const.PRJ.SENSES).getPath()
        run(dtPath="/Users/alex/Desktop/w2v-jbt-nns/dt-text.csv",
            vocPath="/Users/alex/Desktop/w2v-jbt-nns/target.csv")
    }
}
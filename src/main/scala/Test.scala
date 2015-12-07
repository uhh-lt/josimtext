import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import Util.delete
import org.apache.spark.rdd.RDD

object Test {
  def main(args: Array[String]) {
    if (args.size < 1) {
      println("Usage: Test <input>")
     return
    }

    // Input parameters
    val inputPath = args(0)
    val outputPath = inputPath + "-output"
    println("Input DT: " + inputPath)
    Util.delete(outputPath)

    // Set Spark configuration
    val conf = new SparkConf().setAppName("Test job")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // Action
    val dt = sc.textFile(inputPath)
      .map(line => line.split("\t"))
      .map({case Array(word_i, word_j, sim_ij) => (word_i, word_j, sim_ij) case _ => ("?", "?", "?") })
      .saveAsTextFile(outputPath)
  }
}

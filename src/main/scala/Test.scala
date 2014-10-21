import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Test {

  def main(args: Array[String]) {
    val dir = args(0)
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)
    val file = sc.textFile(dir)
    val wordFeatureCounts = file.map(line => line.split("	"))
                                .map(cols => (cols(0), (cols(1), cols(2).toInt)))
                                .filter({case (word, (feature, wfc)) => (wfc >= 2)})
    val wordCounts = wordFeatureCounts.map({case (word, (feature, wfc)) => (word, wfc)})
                                      .reduceByKey((wc1, wc2) => wc1 + wc2)
    print(wordCounts.count)
    wordCounts.cache()
//    val featureCounts = wordFeatureCounts.map({case (word, (feature, wfc)) => (feature, wfc)})
//                                         .reduceByKey((fc1, fc2) => fc1 + fc2)
    wordCounts.cartesian(wordCounts).saveAsTextFile(dir + "__Test");
  }
}

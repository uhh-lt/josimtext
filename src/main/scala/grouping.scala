import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object grouping {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
    val sc = new SparkContext(conf)

    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD
      .reduceByKey(_ + _)
      .collect()

    val wordCountsWithGroup: RDD[(String, Iterable[Int])] = wordPairsRDD
      .groupByKey()

    wordCountsWithGroup
      .map(t => (t._1, t._2.sum))
      .collect()

  }
}

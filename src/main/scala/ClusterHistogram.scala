import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}


object ClusterHistogram {

    def main(args: Array[String]) {
        if (args.size < 3) {
            println("Usage: ClusterHistogram cluster-file min-cluster-size max-num-clusters")
            return
        }

        val conf = new SparkConf().setAppName("ClusterHistogram")
        val sc = new SparkContext(conf)

        val clusterFile = sc.textFile(args(0))
        val minClusterSize = args(1).toInt
        val maxNumClusters = args(2).toInt
        //val numFeatures = args(3).toInt
        //val minPMI = args(4).toDouble
        //val multiplyScores = args(5).toBoolean

        // (lemma, (sense -> (feature -> prob)))
        clusterFile
            .map(line => line.split("\t"))
            .map({case Array(lemma, sense, senseLabel, simWords) => (lemma, (sense.toInt, simWords.split("  ").size))})
            .filter({case (lemma, (sense, numSimWords)) => numSimWords >= minClusterSize})
            .groupByKey()
            .map({case (lemma, clusters) => (math.min(clusters.size, maxNumClusters), 1)})
            .reduceByKey(_+_)
            .sortBy(_._1)
            .map({case (clusterSize, num) => clusterSize + "\t" + num})
            .saveAsTextFile(args(0) + "__Histogram__minClusterSize" + minClusterSize + "__maxNumClusters" + maxNumClusters)
    }
}

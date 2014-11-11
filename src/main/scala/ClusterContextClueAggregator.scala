import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object ClusterContextClueAggregator {
    def main(args: Array[String]) {
        if (args.size < 3) {
            println("Usage: ClusterContextClueAggregator cluster-file feature-file output [min. probability] [min. coverage]")
            return
        }

        val param_s = if (args.length > 3) args(3).toDouble else 0.0
        val param_p = if (args.length > 4) args(4).toDouble else 0.0

        val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
        val sc = new SparkContext(conf)

        val clusterFile = sc.textFile(args(0))
        val featureFile = sc.textFile(args(1))
        val outputFile = args(2)

        val clusterWords:RDD[(String, (String, String))] = clusterFile
            .map(line => line.split("\t"))
            .map(cols => (cols(0), cols(1) + "\t" + cols(2), cols(3).split("  ")))
            .flatMap({case (word, sense, simWords) => for(simWord <- simWords) yield (simWord, (word, sense))})

        val wordFeatures = featureFile
            .map(line => line.split("\t"))
            .map(cols => (cols(0), (cols(1), cols(4).toFloat / cols(3).toFloat, cols(4).toFloat / cols(2).toFloat))) // prob = wfc / fc, coverage = wfc / wc
            .filter({case (word, (feature, prob, coverage)) => prob >= param_s && coverage >= param_p})

        clusterWords
            .join(wordFeatures)
            .map({case (simWord, ((word, sense), (feature, prob, coverage))) => ((word, sense, feature), (prob, 1.0f))})
            .reduceByKey({case ((v1, n1), (v2, n2)) => (v1 + v2, n1 + n2)})
            .map({case ((word, sense, feature), (score, n)) => ((word, sense), (feature, score / n))})
            .groupByKey()
            .mapValues(featureCounts => featureCounts.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2}))
            .map({case ((word, sense), featureCounts) => word + "\t" + sense + "\t" + featureCounts.map({case (feature, score) => feature + ":" + score}).mkString("  ")})
            .saveAsTextFile(outputFile)
    }
}

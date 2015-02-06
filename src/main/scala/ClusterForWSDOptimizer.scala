import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}


object ClusterForWSDOptimizer {

    def computeFeatureValues(word:String, featureValues:String, clusterSize:Int): (String, Double) = {
        val featureArr = Util.splitLastN(featureValues, ':', 2)
        val feature = featureArr(0)
        if (featureArr.length != 2)
            return null
        val prob     = featureArr(1).toDouble // P(c|s_k*)
        if (feature.equals(word))
            return null
        (feature, prob)
    }

    def computeFeatureProbs(word:String, featuresWithValues:Array[String], clusterSize:Int, senseCount:Double, p:Int): Map[String, Double] = {
        val res = featuresWithValues
            .map(featureValues => computeFeatureValues(word, featureValues, clusterSize))
            .filter(_ != null)
            //.map({case (feature, prob) => (feature, prob * prob * senseCount / featureCounts(feature))})
            //.sortBy(_._2)
            //.reverse
            //.take(p)
        res.toMap
    }

    def calcScore(cluster1:(String, (Double, Seq[String], Map[String, Double])), cluster2:(String, (Double, Seq[String], Map[String, Double])), p:Int):Double = {
        val features1 = cluster1._2._3.toSeq.sortBy(_._2).takeRight(p).toMap
        val features2 = cluster2._2._3.toSeq.sortBy(_._2).takeRight(p).toMap
        /*val featureUnion = features1.keySet.union(features2.keySet)
        var totalDiff = 0.0
        var totalProb1 = math.log(cluster1._2._1)
        var totalProb2 = math.log(cluster2._2._1)
        for (feature <- featureUnion) {
            val p1 = if (features1 contains feature) features1(feature) else 0.0
            val p2 = if (features2 contains feature) features2(feature) else 0.0
            val diff = math.abs(p1 - p2)
            val relDiff = diff / math.max(p1,p2)
            totalDiff += relDiff
            totalProb1 = math.log(p1 + 0.00001)
            totalProb2 = math.log(p2 + 0.00001)
        }
        val avgDiff = totalDiff / featureUnion.size
        val probDiff = math.abs(totalProb1 - totalProb2)*/
        val numFeatures = math.max(features1.size, features2.size)
        val featureIntersection = features1.keySet.intersect(features2.keySet).size
        featureIntersection / numFeatures.toDouble
    }

    def pruneClusters(clusters:Seq[(String, (Double, Seq[String], Map[String, Double]))], p:Int, simThreshold:Double):Seq[(String, (Double, Seq[String], Map[String, Double]))] = {
        var clustersPruned:Seq[(String, (Double, Seq[String], Map[String, Double]))] = List()
        for (i <- 0 to clusters.size - 1) {
            val cluster1 = clusters(i)
            val cluster1AvgSim = cluster1._2._2.map(Util.splitLastN(_, ':', 2)(1).toInt).sum.toDouble / cluster1._2._2.size
            var dropCluster = false
            for (j <- 0 to clusters.size - 1) {
                //if (i < j) {
                    val cluster2 = clusters(j)
                    val cluster2AvgSim = cluster2._2._2.map(Util.splitLastN(_, ':', 2)(1).toInt).sum.toDouble / cluster2._2._2.size
                    val sim = calcScore(cluster1, cluster2, p)
                    // Drop only the cluster with lower similarity
                    if (sim >= simThreshold && cluster1AvgSim < cluster2AvgSim) {
                        dropCluster = true
                    }
                //}
            }
            if (!dropCluster) {
                clustersPruned = clustersPruned :+ cluster1
            }
        }
        clustersPruned
    }

    def main(args: Array[String]) {
        if (args.size < 1) {
            println("Usage: ClusterForWSDOptimizer cluster-file-with-clues")
            return
        }

        val conf = new SparkConf().setAppName("ClusterForWSDOptimizer")
        val sc = new SparkContext(conf)

        val clusterFile = sc.textFile(args(0))
        val p = args(1).toInt
        val simThreshold = args(2).toDouble
        val outputFile = args(0) + "__p" + p + "__s" + simThreshold

        /*val featureCounts = featureFile
            .map(line => line.split("\t"))
            .map({case Array(word, count) => (word, count.toInt)})
            .filter({case (word, count) => count >= 2})
            .collect()
            .toMap*/

        // (lemma, (sense -> (feature -> prob)))
        val clustersWithClues = clusterFile
            .map(line => line.split("\t"))
            .map({case Array(lemma, sense, senseLabel, senseCount, simWords, featuresWithValues) => (lemma, sense + "\t" + senseLabel, senseCount.toDouble, simWords.split("  "), featuresWithValues.split("  "))})
            .map({case (lemma, sense, senseCount, simWords, featuresWithValues) => (lemma, (sense, (senseCount, simWords.toSeq, computeFeatureProbs(lemma, featuresWithValues, simWords.size, senseCount, p))))})
            .groupByKey()
            .mapValues(clusters => pruneClusters(clusters.toSeq, p, simThreshold))
            .flatMap({case (lemma, clusters) => for (cluster <- clusters) yield (lemma, cluster)})
            .sortBy({case (lemma, cluster) => (lemma, cluster._1)})

        clustersWithClues
            .map({case (word, (sense, (senseCount, simWordsWithSim, senseFeatureProbs))) =>
                word + "\t" +
                sense + "\t" +
                senseCount + "\t" +
                simWordsWithSim.mkString("  ") + "\t" +
                senseFeatureProbs.map(tuple => tuple._1 + ":" + tuple._2).mkString("  ")})
            .saveAsTextFile(outputFile)
    }
}

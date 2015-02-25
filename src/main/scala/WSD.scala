import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}


object WSD {

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

    def computeFeatureProbs(word:String, featuresWithValues:Array[String], clusterSize:Int, senseCount:Double): Map[String, Double] = {
        val res = featuresWithValues
            .map(featureValues => computeFeatureValues(word, featureValues, clusterSize))
            .filter(_ != null)
            //.sortBy({case (feature, (sfc, fc)) => (sfc * sfc)/(fc * senseCount)})
            //.reverse
            //.take(1000)
        res.toMap
    }

    def chooseSense(contextFeatures:Set[String], senseInfoCoocs:Map[Int, (Double, Int, Map[String, Double])], senseInfoDeps:Map[Int, (Double, Int, Map[String, Double])], alpha:Double, wsdMode:WSDMode.WSDMode):Int = {
        val senseProbs = collection.mutable.Map[Int, Double]() // Probabilities to compute
        val senses = if (senseInfoDeps != null) senseInfoCoocs.keys.toSet.intersect(senseInfoDeps.keys.toSet) else senseInfoCoocs.keys

        for (sense <- senseInfoCoocs.keys) {
            // we simply ignore the (1/N_w) factor here, as it is constant for all senses
            val senseCount = senseInfoCoocs(sense)._1 // * (1/N_w)
            //val clusterSize = senseInfo(sense)._2
            if (wsdMode == WSDMode.Product) {
                senseProbs(sense) = math.log(senseCount)
            } else {
                senseProbs(sense) = 0
            }
        }
        //var atLeastOneFeatureFound = false
        // p(s|f1..fn) = 1/p(f1..fn) * p(s) * p(f1|s) * .. * p(fn|s)
        // where 1/p(f1..fn) is ignored as it is equal for all senses
        for (sense <- senses) {
            val featureSenseProbsCoocs = senseInfoCoocs(sense)._3
            val featureSenseProbsDeps = if (senseInfoDeps != null) senseInfoDeps(sense)._3 else Map[String, Double]()
            for (feature <- contextFeatures) {
                // Smoothing for previously unseen context features
                var featureProb = alpha
                if (featureSenseProbsCoocs.contains(feature)) {
                    //atLeastOneFeatureFound = true
                    featureProb += featureSenseProbsCoocs(feature)
                } else if (featureSenseProbsDeps.contains(feature)) {
                    //atLeastOneFeatureFound = true
                    featureProb += featureSenseProbsDeps(feature)
                }
                senseProbs(sense) += math.log(featureProb)
            }
        }

        // return index of most probable sense (if no feature overlap, equals sense with highest prior prob)
        //if (atLeastOneFeatureFound)
            senseProbs.toList.sortBy(_._2).last._1
        //else
        //    -1
    }

    def computeMatchingScore[T](matchingCountsSorted:Array[(T, Int)]):(Int, Int) = {
        val countSum = matchingCountsSorted.map(matchCount => matchCount._2).sum
        // return "highest count" / "sum of all counts"
        (matchingCountsSorted.head._2, countSum)
    }

    def mappingToClusters[A, B](mapping:Iterable[(A, B)]):Seq[Set[A]] = {
        mapping.groupBy(_._2)
               .toSeq
               .map({case (b,aAndBs) => aAndBs.map(_._1).toSet})
    }

    def pruneClusters[C](clusters:Iterable[(Int, (Double, Int, C))], maxNumClusters:Int):Iterable[(Int, (Double, Int, C))] = {
        clusters.toList.sortBy({case (sense, (senseCount, clusterSize, _)) => clusterSize}).reverse.take(maxNumClusters)
    }

    def main(args: Array[String]) {
        if (args.size < 5) {
            println("Usage: WSDEvaluation clusters-with-coocs clusters-with-deps linked-sentences-tokenized output prob-smoothing-addend wsd-mode")
            return
        }

        // TODO: Options for
        // TODO: - whether to take into account the prior probability
        // TODO: - weighting of similar words (e.g.: depending on rank, depending on similarity; both of these to the power of X)

        val conf = new SparkConf().setAppName("WSDEvaluation")
        val sc = new SparkContext(conf)

        val clusterFileCoocs = sc.textFile(args(0))
        val clusterFileDeps = if (args.size == 6) sc.textFile(args(1)) else null
        val sentFile = sc.textFile(args(args.size - 4))
        val outputFile = args(args.size - 3)
        val alpha = args(args.size - 2).toDouble
        val wsdMode = WSDMode.withName(args(args.size - 1))
        //val minClusterSize = args(5).toInt
        //val maxNumClusters = args(6).toInt
        //val featureCol = args(5).toInt
        //val numFeatures = args(3).toInt
        //val minPMI = args(4).toDouble
        //val multiplyScores = args(5).toBoolean

        val sentLinkedTokenized = sentFile
            .map(line => line.split("\t", -1)) // -1 means "do not drop empty entries"
            .zipWithIndex()               // (lemma,       (sentId, target,      features))
            .map({case (sentLine, sentId) => (sentLine(0), (sentId, sentLine(1), sentLine(2).split(" ") ++ sentLine(3).split(" ")))})
            .cache()

        // (lemma, (sense -> (feature -> prob)))
        val clustersWithCoocs:RDD[(String, Map[Int, (Double, Int, Map[String, Double])])] = clusterFileCoocs
            .map(line => line.split("\t"))
            .map({case Array(lemma, sense, senseLabel, senseCount, simWords, featuresWithValues) => (lemma, sense.toInt, senseCount.toDouble, simWords.split("  "), featuresWithValues.split("  "))})
            //.filter({case (lemma, sense, senseCount, simWords, featuresWithValues) => simWords.size >= minClusterSize})
            .map({case (lemma, sense, senseCount, simWords, featuresWithValues) => (lemma, (sense, (senseCount, simWords.size, computeFeatureProbs(lemma, featuresWithValues, simWords.size, senseCount))))})
            .groupByKey()
            .mapValues(clusters => /*pruneClusters(clusters, maxNumClusters)*/clusters.toMap)

        var clustersWithDeps: RDD[(String, Map[Int, (Double, Int, Map[String, Double])])] = null
        if (clusterFileDeps != null) {
            // (lemma, (sense -> (feature -> prob)))
            clustersWithDeps = clusterFileDeps
                .map(line => line.split("\t"))
                .map({ case Array(lemma, sense, senseLabel, senseCount, simWords, featuresWithValues) => (lemma, sense.toInt, senseCount.toDouble, simWords.split("  "), featuresWithValues.split("  "))})
                //.filter({case (lemma, sense, senseCount, simWords, featuresWithValues) => simWords.size >= minClusterSize})
                .map({ case (lemma, sense, senseCount, simWords, featuresWithValues) => (lemma, (sense, (senseCount, simWords.size, computeFeatureProbs(lemma, featuresWithValues, simWords.size, senseCount))))})
                .groupByKey()
                .mapValues(clusters => /*pruneClusters(clusters, maxNumClusters)*/ clusters.toMap)
        }

        var sentLinkedTokenizedContextualized: RDD[(String, Long, String, Int, Array[String])] = null
        if (clustersWithDeps != null) {
            sentLinkedTokenizedContextualized = sentLinkedTokenized
                .join(clustersWithCoocs)
                .join(clustersWithDeps)
                .map({case (lemma, (((sentId, target, tokens), senseInfoCoocs), senseInfoDeps)) => (lemma, sentId, target, chooseSense(tokens.toSet, senseInfoCoocs, senseInfoDeps, alpha, wsdMode), tokens)})
                .cache()
        } else {
            sentLinkedTokenizedContextualized = sentLinkedTokenized
                .join(clustersWithCoocs)
                .map({case (lemma, ((sentId, target, tokens), senseInfoCoocs)) => (lemma, sentId, target, chooseSense(tokens.toSet, senseInfoCoocs, null, alpha, wsdMode), tokens)})
                .cache()
        }

        sentLinkedTokenizedContextualized
            .map({case (lemma, sentId, target, sense, tokens) => lemma + "\t" + target + "\t" + sense + "\t" + tokens.mkString(" ")})
            .saveAsTextFile(outputFile + "/Contexts")
    }
}

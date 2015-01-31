import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

object WSDMode extends Enumeration {
    type WSDMode = Value
    val Product, Maximum, Average = Value
}

object WSDEvaluation {

    /*def computeFeatureProb(featureArr:Array[String], clusterSize:Int): (String, Double, Double) = {
        val feature = featureArr(0)
        if (featureArr.length != 8)
            return (feature, 0, 0)
        //val lmi     = featureArr(1).toFloat
        //val avgProb = featureArr(2).toFloat // (p(w1|f) + .. + p(wn|f)) / n
        //val avgCov  = featureArr(3).toFloat // (p(f|w1) + .. + p(f|wn)) / n
        val sc      = featureArr(4).toLong  // c(w1) + .. + c(wn) = c(s)
        val fc      = featureArr(5).toLong  // c(f)
        //val avgWc   = wc.toFloat / clusterSize // c(s) / n = (c(w1) + .. + c(wn)) / n
        val sfc     = featureArr(6).toLong // c(s,f)
        //val totalNumObservations = featureArr(7).toLong
        //val normalizedAvgWfc = avgCov * avgWc // (p(f|w1) + .. + p(f|wn))/n * c(s)/n = (c(w1,f) + .. + c(wn,f))/n = c(s,f)/n
        //val score = (normalizedAvgWfc * normalizedAvgWfc) / (avgWc * fc)
        //val pmi = (avgProb * totalNumObservations) / wc; // p(w|f)*n / c(w) = c(w|f) / c(w) = pmi(w,f)
        (feature, 0, 0)
    }

    def computeFeatureProbs(featuresWithValues:Array[String], numFeatures:Int, clusterSize:Int): Map[String, Double] = {
        featuresWithValues
            .map(featureArr => computeFeatureProb(featureArr.split(":"), clusterSize))
            .sortBy({case (feature, avgProb, score) => score})
            .reverse
            .take(numFeatures)
            .map({case (feature, avgProb, score) => (feature, avgProb)})
            .toMap
    }*/

    /**
     * Computes the NMI (normalized mutual information) of two clustering of the same N documents.
     * A clustering here is an aggregation of documents into multiple sets (clusters).
     * @tparam D    Type of document representation (e.g. Int or Long as document IDs)
     * @param clustering1 First clustering
     * @param clustering2 Second clustering
     * @param N     Number of documents
     * @return      NMI of both clusterings
     */
    def nmi[D](clustering1:Seq[Set[D]], clustering2:Seq[Set[D]], N:Int): Double = {
        var H1 = 0.0
        var H2 = 0.0
        for (docIDs <- clustering1) {
            val n = docIDs.size
            val p = n.toDouble / N.toDouble
            H1 -= p * math.log(p)
        }
        for (docIDs <- clustering2  ) {
            val n = docIDs.size
            val p = n.toDouble / N.toDouble
            H2 -= p * math.log(p)
        }

        var I = 0.0
        for (docIDs1 <- clustering1; docIDs2 <- clustering2) {
            val p_ij = docIDs1.intersect(docIDs2).size / N.toDouble
            val p_i = docIDs1.size / N.toDouble
            val p_j = docIDs2.size / N.toDouble

            if (p_ij != 0) {
                I += p_ij * math.log(p_ij / (p_i * p_j))
            }
        }

        2*I / (H1 + H2)
    }

    def splitLastN(text:String, del:String, n:Int):Array[String] = {
        val featureArr = new Array[String](3)
        var lastPos = text.length
        for (i <- (0 to n-1).reverse) {
            // for the first element we position an imaginary delimeter at position -1
            val nextPos = if (i == 0) -1 else text.lastIndexOf(del, lastPos-1)
            featureArr(i) = text.substring(nextPos+1, lastPos)
            lastPos = nextPos
        }
        featureArr
    }

    def computeFeatureValues(word:String, featureValues:String, clusterSize:Int): (String, (Double, Double)) = {
        val featureArr = splitLastN(featureValues, ":", 3)
        val feature = featureArr(0)
        if (featureArr.length != 3)
            return (feature, (0, 1))
        //val lmi     = featureArr(1).toFloat
        //val avgProb = featureArr(2).toFloat // (p(w1|f) + .. + p(wn|f)) / n
        //val avgCov  = featureArr(3).toFloat // (p(f|w1) + .. + p(f|wn)) / n
        //val sc      = featureArr(1).toLong  // c(w1) + .. + c(wn) = c(s)
        //val fc      = featureArr(5).toLong  // c(f)
        //val avgWc   = wc.toFloat / clusterSize // c(s) / n = (c(w1) + .. + c(wn)) / n
        var sfc     = featureArr(1).toDouble / clusterSize // c(s,f)
        val fc      = featureArr(2).toDouble // c(f)
        if (feature.equals(word))
            sfc = 0
        //val totalNumObservations = featureArr(7).toLong
        //val normalizedAvgWfc = avgCov * avgWc // (p(f|w1) + .. + p(f|wn))/n * c(s)/n = (c(w1,f) + .. + c(wn,f))/n = c(s,f)/n
        //val score = (normalizedAvgWfc * normalizedAvgWfc) / (avgWc * fc)
        //val pmi = (avgProb * totalNumObservations) / wc; // p(w|f)*n / c(w) = c(w|f) / c(w) = pmi(w,f)
        (feature, (sfc, fc))
    }

    def computeFeatureProbs(word:String, featuresWithValues:Array[String], clusterSize:Int): Map[String, (Double, Double)] = {
        featuresWithValues
            .map(featureValues => computeFeatureValues(word, featureValues, clusterSize))
            //.sortBy({case (feature, (sfc, fc)) => (sfc * sfc)/fc.toDouble})
            //.reverse
            //.take(10000)
            .toMap
    }

    def chooseSense(contextFeatures:Set[String], senseInfo:Map[Int, (Double, Map[String, (Double, Double)])], alpha:Double, wsdMode:WSDMode.WSDMode):Int = {
        val senseProbs = collection.mutable.Map[Int, Double]()
        val J = senseInfo.size
        for (sense <- senseInfo.keys) {
            val senseCount = senseInfo(sense)._1
            if (wsdMode == WSDMode.Product) {
                senseProbs(sense) = (J - 1) * math.log(senseCount)
            } else {
                senseProbs(sense) = 0
            }
        }
        var atLeastOneFeatureFound = false
        // p(s|f1..fn) = 1/p(f1..fn) * p(s) * p(f1|s) * .. * p(fn|s)
        // where 1/p(f1..fn) is ignored as it is equal for all senses
        for (sense <- senseInfo.keys) {
            val featureSenseCounts = senseInfo(sense)._2
            for (feature <- contextFeatures) {
                // Smoothing for previously unseen context features
                var featureSenseCount = if (wsdMode == WSDMode.Product) alpha else 0
                var featureCount = 1.0 // arbitrary default value != 0 if feature is not present (0/X is 0)
                if (featureSenseCounts.contains(feature)) {
                    atLeastOneFeatureFound = true
                    featureSenseCount += featureSenseCounts(feature)._1
                    featureCount = featureSenseCounts(feature)._2
                }
                if (wsdMode == WSDMode.Product) {
                    senseProbs(sense) += math.log(featureSenseCount)
                } else if (wsdMode == WSDMode.Average) {
                    senseProbs(sense) += featureSenseCount / featureCount
                } else if (wsdMode == WSDMode.Maximum) {
                    senseProbs(sense) = math.max(senseProbs(sense), featureSenseCount / featureCount)
                }
            }
        }

        // return index of most probable sense
        if (atLeastOneFeatureFound)
            senseProbs.toList.sortBy(_._2).last._1
        else
            -1
    }

    def computeMatchingScore[T](matchingCountsSorted:Array[(T, Int)]):(Int, Int) = {
        val countSum = matchingCountsSorted.map(matchCount => matchCount._2).fold(0)(_+_)
        // return "highest count" / "sum of all counts"
        (matchingCountsSorted.head._2, countSum)
    }

    def mappingToClusters[A, B](mapping:Iterable[(A, B)]):Seq[Set[A]] = {
        mapping.groupBy(_._2)
               .toSeq
               .map({case (b,aAndBs) => aAndBs.map(_._1).toSet})
    }

    def main(args: Array[String]) {
        if (args.size < 6) {
            println("Usage: WSDEvaluation cluster-file-with-clues linked-sentences-tokenized output prob-smoothing-addend wsd-mode min-cluster-size")
            return
        }

        val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
        val sc = new SparkContext(conf)

        val clusterFile = sc.textFile(args(0))
        val sentFile = sc.textFile(args(1))
        val outputFile = args(2)
        val alpha = args(3).toDouble
        val wsdMode = WSDMode.withName(args(4))
        val minClusterSize = args(5).toInt
        //val numFeatures = args(3).toInt
        //val minPMI = args(4).toDouble
        //val multiplyScores = args(5).toBoolean

        val sentLinkedTokenized = sentFile
            .map(line => line.split("\t"))
            .zipWithIndex()
            .map({case (Array(lemma, target, tokens), sentId) => (lemma, (sentId, target, tokens.split(" ")))})

        // (lemma, (sense -> (feature -> prob)))
        val clustersWithClues:RDD[(String, Map[Int, (Double, Map[String, (Double, Double)])])] = clusterFile
            .map(line => line.split("\t"))
            .map({case Array(lemma, sense, senseLabel, senseCount, simWords, featuresWithValues) => (lemma, sense, senseLabel, senseCount, simWords.split("  "), featuresWithValues.split("  "))})
            .filter({case (lemma, sense, senseLabel, senseCount, simWords, featuresWithValues) => simWords.size >= minClusterSize})
            .map({case (lemma, sense, senseLabel, senseCount, simWords, featuresWithValues) => (lemma, (sense.toInt, (senseCount.toDouble / simWords.size, computeFeatureProbs(lemma, featuresWithValues, simWords.size))))})
            .groupByKey()
            .mapValues(senseInfo => senseInfo.toMap)

        val sentLinkedTokenizedContextualized = sentLinkedTokenized
            .join(clustersWithClues)
            .map({case (lemma, ((sentId, target, tokens), senseInfo)) => (lemma, sentId, target, chooseSense(tokens.toSet, senseInfo, alpha, wsdMode), tokens)})
            .cache()

        val goldClustering = sentLinkedTokenizedContextualized
            .map({case (lemma, sentId, target, sense, tokens) => (lemma, (sentId, target))})
            .groupByKey()
            .mapValues(mappingToClusters)

        val testClustering = sentLinkedTokenizedContextualized
            .map({case (lemma, sentId, target, sense, tokens) => (lemma, (sentId, sense))})
            .groupByKey()
            .mapValues(mappingToClusters)

        val nmiScores = goldClustering.join(testClustering)
            .mapValues(clusterings => nmi(clusterings._1, clusterings._2, 100))

        nmiScores.map({case (lemma, nmiScore) => lemma + "\t" + nmiScore})
                 .saveAsTextFile(outputFile + "/NMIPerLemma")

        nmiScores.map({case (lemma, nmiScore) => ("FOO", (nmiScore, 1))})
                 .reduceByKey({case ((a1,b1), (a2,b2)) => (a1+a2,b1+b2)})
                 .map({case (_, (avgNmiScore, numLemmas)) => "AVG_NMI\t" + avgNmiScore / numLemmas})
                 .saveAsTextFile(outputFile + "/NMIPerLemma__Total")

        sentLinkedTokenizedContextualized
            .map({case (lemma, sentId, target, sense, tokens) => lemma + "\t" + target + "\t" + sense + "\t" + tokens.mkString(" ")})
            .saveAsTextFile(outputFile + "/Contexts")

        val senseTargetCounts = sentLinkedTokenizedContextualized
            .map({case (lemma, sentId, target, sense, tokens) => ((lemma, target, sense), 1)})
            .reduceByKey(_+_)
            .cache()



        // SENSE -> MOST FREQ. TARGET
        val targetsPerSense = senseTargetCounts
            .map({case ((lemma, target, sense), count) => ((lemma, sense), (target, count))})
            .groupByKey()
            .map({case ((lemma, sense), targetCounts) => ((lemma, sense), targetCounts.toArray.sortBy(_._2).reverse)})

        targetsPerSense
            .map({case ((lemma, sense), targetCounts) => lemma + "\t" + sense + "\t" + targetCounts.map(targetCount => targetCount._1 + ":" + targetCount._2).mkString("  ")})
            .saveAsTextFile(outputFile + "/TargetsPerSense")

        val targetsPerSenseResults = targetsPerSense
            .map({case ((lemma, sense), targetCounts) => (lemma, computeMatchingScore(targetCounts))})
            .reduceByKey({case ((correct1, total1), (correct2, total2)) => (correct1+correct2, total1+total2)})

        targetsPerSenseResults
            .map({case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
            .saveAsTextFile(outputFile + "/TargetsPerSense__Results")

        targetsPerSenseResults
            .map({case (lemma, (correct, total)) => ("TOTAL", (correct, total))})
            .reduceByKey({case ((correct1, total1), (correct2, total2)) => (correct1+correct2, total1+total2)})
            .map({case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
            .saveAsTextFile(outputFile + "/TargetsPerSense__ResultsAggregated")



        // TARGET -> MOST FREQ. SENSE
        val sensesPerTarget = senseTargetCounts
            .map({case ((lemma, target, sense), count) => ((lemma, target), (sense, count))})
            .groupByKey()
            .map({case ((lemma, target), senseCounts) => ((lemma, target), senseCounts.toArray.sortBy(_._2).reverse)})

        sensesPerTarget
            .map({case ((lemma, target), senseCounts) => lemma + "\t" + target + "\t" + senseCounts.map(senseCount => senseCount._1 + ":" + senseCount._2).mkString("  ")})
            .saveAsTextFile(outputFile + "/SensesPerTarget")

        val sensesPerTargetResults = sensesPerTarget
            .map({case ((lemma, target), senseCounts) => (lemma, computeMatchingScore(senseCounts))})
            .reduceByKey({case ((correct1, total1), (correct2, total2)) => (correct1+correct2, total1+total2)})

        sensesPerTargetResults
            .map({case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
            .saveAsTextFile(outputFile + "/SensesPerTarget__Results")

        sensesPerTargetResults
            .map({case (lemma, (correct, total)) => ("TOTAL", (correct, total))})
            .reduceByKey({case ((correct1, total1), (correct2, total2)) => (correct1+correct2, total1+total2)})
            .map({case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
            .saveAsTextFile(outputFile + "/SensesPerTarget__ResultsAggregated")
    }
}

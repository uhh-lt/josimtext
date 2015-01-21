import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

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

    def computeFeatureValues(featureArr:Array[String], clusterSize:Int): (String, Double) = {
        val feature = featureArr(0)
        if (featureArr.length != 3)
            return (feature, 0L)
        //val lmi     = featureArr(1).toFloat
        //val avgProb = featureArr(2).toFloat // (p(w1|f) + .. + p(wn|f)) / n
        //val avgCov  = featureArr(3).toFloat // (p(f|w1) + .. + p(f|wn)) / n
        //val sc      = featureArr(1).toLong  // c(w1) + .. + c(wn) = c(s)
        //val fc      = featureArr(5).toLong  // c(f)
        //val avgWc   = wc.toFloat / clusterSize // c(s) / n = (c(w1) + .. + c(wn)) / n
        val sfc     = featureArr(2).toDouble / clusterSize // c(s,f)
        //val totalNumObservations = featureArr(7).toLong
        //val normalizedAvgWfc = avgCov * avgWc // (p(f|w1) + .. + p(f|wn))/n * c(s)/n = (c(w1,f) + .. + c(wn,f))/n = c(s,f)/n
        //val score = (normalizedAvgWfc * normalizedAvgWfc) / (avgWc * fc)
        //val pmi = (avgProb * totalNumObservations) / wc; // p(w|f)*n / c(w) = c(w|f) / c(w) = pmi(w,f)
        (feature, sfc)
    }

    def computeFeatureProbs(featuresWithValues:Array[String], clusterSize:Int): Map[String, Double] = {
        featuresWithValues
            .map(featureArr => computeFeatureValues(featureArr.split(":"), clusterSize))
            .toMap
    }

    def chooseSense(contextFeatures:Set[String], senseInfo:Map[Int, (Long, Map[String, Double])], alpha:Double):Int = {
        val senseProbs = collection.mutable.Map[Int, Double]()
        val J = senseInfo.size
        for (sense <- senseInfo.keys) {
            val senseCount = senseInfo(sense)._1
            senseProbs(sense) = (J - 1) * math.log(senseCount)
        }
        // p(s|f1..fn) = 1/p(f1..fn) * p(s) * p(f1|s) * .. * p(fn|s)
        // where 1/p(f1..fn) is ignored as it is equal for all senses
        for (sense <- senseInfo.keys) {
            val featureSenseCounts = senseInfo(sense)._2
            for (feature <- contextFeatures) {
                // Smoothing for previously unseen context features
                var featureSenseCount = alpha
                if (featureSenseCounts.contains(feature)) {
                    featureSenseCount += featureSenseCounts(feature)
                }
                senseProbs(sense) += math.log(featureSenseCount)
            }
        }

        // return index of most probable sense
        senseProbs.toList.sortBy(_._2).last._1
    }

    def computeMatchingScore[T](matchingCountsSorted:Array[(T, Int)]):(Int, Int) = {
        val countSum = matchingCountsSorted.map(matchCount => matchCount._2).fold(0)(_+_)
        // return "highest count" / "sum of all counts"
        (matchingCountsSorted.head._2, countSum)
    }

    def main(args: Array[String]) {
        if (args.size < 3) {
            println("Usage: WSDEvaluation cluster-file-with-clues linked-sentences-tokenized output prob-smoothing-addend")
            return
        }

        val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
        val sc = new SparkContext(conf)

        val clusterFile = sc.textFile(args(0))
        val sentFile = sc.textFile(args(1))
        val outputFile = args(2)
        val alpha = args(3).toDouble
        //val numFeatures = args(3).toInt
        //val minPMI = args(4).toDouble
        //val multiplyScores = args(5).toBoolean

        val sentLinkedTokenized = sentFile
            .map(line => line.split("\t"))
            .map({case Array(lemma, target, tokens) => (lemma, (target, tokens.split(" ").toSet))})

        // (lemma, (sense -> (feature -> prob)))
        val clustersWithClues:RDD[(String, Map[Int, (Long, Map[String, Double])])] = clusterFile
            .map(line => line.split("\t"))
            .map({case Array(lemma, sense, senseLabel, senseCount, simWords, featuresWithValues) => (lemma, (sense.toInt, (senseCount.toLong, computeFeatureProbs(featuresWithValues.split("  "), simWords.size))))})
            .groupByKey()
            .mapValues(senseInfo => senseInfo.toMap)

        val sentLinkedTokenizedContextualized = sentLinkedTokenized
            .join(clustersWithClues)
            .map({case (lemma, ((target, tokens), senseInfo)) => (lemma, target, chooseSense(tokens, senseInfo, alpha), tokens)})

        sentLinkedTokenizedContextualized
            .saveAsTextFile(outputFile + "/Contexts")

        val senseTargetCounts = sentLinkedTokenizedContextualized
            .map({case (lemma, target, sense, tokens) => ((lemma, target, sense), 1)})
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

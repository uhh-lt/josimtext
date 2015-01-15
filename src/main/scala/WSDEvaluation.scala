import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

object WSDEvaluation {
    def computeFeatureProb(featureArr:Array[String], numFeatures:Int, clusterSize:Int): (String, Double, Double) = {
        val feature = featureArr(0)
        if (featureArr.length != 8)
            return (feature, 0, 0)
        //val lmi     = featureArr(1).toFloat
        val avgProb = featureArr(2).toFloat
        val avgCov  = featureArr(3).toFloat
        val wc      = featureArr(4).toLong
        val fc      = featureArr(5).toLong
        val avgWc   = wc.toFloat / clusterSize
        //val wfc     = featureArr(6).toLong
        //val n       = featureArr(7).toLong
        val normalizedAvgWfc = avgCov * avgWc
        val score = (normalizedAvgWfc * normalizedAvgWfc) / (avgWc * fc)
        (feature, avgProb, score)
    }

    def computeFeatureProbs(featuresWithValues:Array[String], numFeatures:Int, clusterSize:Int): Map[String, Double] = {
        featuresWithValues
            .map(featureArr => computeFeatureProb(featureArr.split(":"), numFeatures, clusterSize))
            .sortBy({case (feature, avgProb, score) => score})
            .reverse
            .take(numFeatures)
            .map({case (feature, avgProb, score) => (feature, avgProb)})
            .toMap
    }

    def chooseSense(contextFeatures:Set[String], featureProbsPerSense:Map[Int, Map[String, Double]]):Int = {
        val senseScores = collection.mutable.Map[Int, Double]().withDefaultValue(0.0)
        senseScores(-1) = 0 // fall-back sense indicates that no context features match one of the senses
        for (feature <- contextFeatures) {
            for (sense <- featureProbsPerSense.keys) {
                if (featureProbsPerSense(sense).contains(feature)) {
                    val featureProb = featureProbsPerSense(sense)(feature)
                    senseScores(sense) += featureProb
                }
            }
        }

        senseScores.toList.sortBy(_._2).last._1 // return index of highest-scoring sense
    }

    def computeMatchingScore[T](matchingCountsSorted:Array[(T, Int)]):(Int, Int) = {
        val countSum = matchingCountsSorted.map(matchCount => matchCount._2).fold(0)(_+_)
        // return "highest count" / "sum of all counts"
        (matchingCountsSorted.head._2, countSum)
    }

    def main(args: Array[String]) {
        if (args.size < 3) {
            println("Usage: WSDEvaluation cluster-file-with-clues linked-sentences-tokenized output")
            return
        }

        val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
        val sc = new SparkContext(conf)

        val clusterFile = sc.textFile(args(0))
        val sentFile = sc.textFile(args(1))
        val outputFile = args(2)
        val numFeatures = args(3).toInt

        val sentLinkedTokenized = sentFile
            .map(line => line.split("\t"))
            .map({case Array(lemma, target, tokens) => (lemma, (target, tokens.split(" ").toSet))})

        // (lemma, (sense -> (feature -> prob)))
        val clustersWithClues:RDD[(String, Map[Int, Map[String, Double]])] = clusterFile
            .map(line => line.split("\t"))
            .map({case Array(lemma, sense, senseLabel, simWords, featuresWithValues) => (lemma, (sense.toInt, computeFeatureProbs(featuresWithValues.split("  "), numFeatures, simWords.size)))})
            .groupByKey()
            .mapValues(featureProbsPerSense => featureProbsPerSense.toMap)

        val sentLinkedTokenizedContextualized = sentLinkedTokenized
            .join(clustersWithClues)
            .map({case (lemma, ((target, tokens), featureProbsPerSense)) => (lemma, target, chooseSense(tokens, featureProbsPerSense), tokens)})

        sentLinkedTokenizedContextualized
            .saveAsTextFile(outputFile + "__Contexts")

        val senseTargetCounts = sentLinkedTokenizedContextualized
            .map({case (lemma, target, sense, tokens) => ((lemma, target, sense), 1)})
            .reduceByKey({case (s1, s2) => s1 + s2})


        // SENSE -> TARGET
        val targetsPerSense = senseTargetCounts
            .map({case ((lemma, target, sense), count) => ((lemma, sense), (target, count))})
            .groupByKey()
            .map({case ((lemma, sense), targetCounts) => ((lemma, sense), targetCounts.toArray.sortBy(_._2).reverse)})

        targetsPerSense
            .map({case ((lemma, sense), targetCounts) => lemma + "\t" + sense + "\t" + targetCounts.map(targetCount => targetCount._1 + ":" + targetCount._2).mkString("  ")})
            .saveAsTextFile(outputFile + "__TargetsPerSense")

        val targetsPerSenseResults = targetsPerSense
            .map({case ((lemma, sense), targetCounts) => (lemma, computeMatchingScore(targetCounts))})
            .reduceByKey({case ((correct1, total1), (correct2, total2)) => (correct1+correct2, total1+total2)})

        targetsPerSenseResults
            .map({case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
            .saveAsTextFile(outputFile + "__TargetsPerSense__Results")

        targetsPerSenseResults
            .map({case (lemma, (correct, total)) => ("TOTAL", (correct, total))})
            .reduceByKey({case ((correct1, total1), (correct2, total2)) => (correct1+correct2, total1+total2)})
            .map({case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
            .saveAsTextFile(outputFile + "__TargetsPerSense__ResultsAggregated")



        // TARGET -> SENSE
        val sensesPerTarget = senseTargetCounts
            .map({case ((lemma, target, sense), count) => ((lemma, target), (sense, count))})
            .groupByKey()
            .map({case ((lemma, target), senseCounts) => ((lemma, target), senseCounts.toArray.sortBy(_._2).reverse)})

        sensesPerTarget
            .map({case ((lemma, target), senseCounts) => lemma + "\t" + target + "\t" + senseCounts.map(senseCount => senseCount._1 + ":" + senseCount._2).mkString("  ")})
            .saveAsTextFile(outputFile + "__SensesPerTarget")

        val sensesPerTargetResults = sensesPerTarget
            .map({case ((lemma, target), senseCounts) => (lemma, computeMatchingScore(senseCounts))})
            .reduceByKey({case ((correct1, total1), (correct2, total2)) => (correct1+correct2, total1+total2)})

        sensesPerTargetResults
            .map({case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
            .saveAsTextFile(outputFile + "__SensesPerTarget__Results")
    }
}

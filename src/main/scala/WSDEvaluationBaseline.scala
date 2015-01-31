import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object WSDEvaluationBaseline {
    def chooseSense(numSenses:Int):Int = {
        Random.nextInt(numSenses)
    }

    def computeMatchingScore[T](matchingCountsSorted:Array[(T, Int)]):(Int, Int) = {
        val countSum = matchingCountsSorted.map(matchCount => matchCount._2).fold(0)(_+_)
        // return "highest count" / "sum of all counts"
        (matchingCountsSorted.head._2, countSum)
    }

    def main(args: Array[String]) {
        if (args.size < 2) {
            println("Usage: WSDEvaluationBaseline linked-sentences-tokenized output")
            return
        }

        val conf = new SparkConf().setAppName("ClusterContextClueAggregator")
        val sc = new SparkContext(conf)

        val sents = sc.textFile(args(0))
            .map(line => line.split("\t"))
            .zipWithIndex()
            .cache()
        //val numSenses = args(2).toInt
        //val numFeatures = args(3).toInt
        //val minPMI = args(4).toDouble
        //val multiplyScores = args(5).toBoolean

        for (numSenses <- List(1,2,3,4,5,6,7,8,9,10,15,20,50,100)) {
            val outputFile = args(1) + "__s" + numSenses

            val sentLinkedTokenizedContextualized = sents
                .map({ case (Array(lemma, target, tokens), sentId) => (lemma, sentId, target, chooseSense(numSenses))})

            sentLinkedTokenizedContextualized
                .saveAsTextFile(outputFile + "/Contexts")

            val senseTargetCounts = sentLinkedTokenizedContextualized
                .map({ case (lemma, sentId, target, sense) => ((lemma, target, sense), 1)})
                .reduceByKey(_ + _)
                .cache()

            val goldClustering = sentLinkedTokenizedContextualized
                .map({ case (lemma, sentId, target, sense) => ("NMI", (sentId, target))})
                .groupByKey()
                .mapValues(WSDEvaluation.mappingToClusters)


            val baselineClustering = sentLinkedTokenizedContextualized
                .map({ case (lemma, sentId, target, sense) => ("NMI", (sentId, sense))})
                .groupByKey()
                .mapValues(WSDEvaluation.mappingToClusters)

            val nmiScoresBaseline = goldClustering.join(baselineClustering)
                .mapValues(clusterings => WSDEvaluation.nmi(clusterings._1, clusterings._2, 100))

            nmiScoresBaseline.map({ case (_, nmiScore) => nmiScore})
                .saveAsTextFile(outputFile + "/NMI")



            // SENSE -> MOST FREQ. TARGET
            val targetsPerSense = senseTargetCounts
                .map({ case ((lemma, target, sense), count) => ((lemma, sense), (target, count))})
                .groupByKey()
                .map({ case ((lemma, sense), targetCounts) => ((lemma, sense), targetCounts.toArray.sortBy(_._2).reverse)})

            targetsPerSense
                .map({ case ((lemma, sense), targetCounts) => lemma + "\t" + sense + "\t" + targetCounts.map(targetCount => targetCount._1 + ":" + targetCount._2).mkString("  ")})
                .saveAsTextFile(outputFile + "/TargetsPerSense")

            val targetsPerSenseResults = targetsPerSense
                .map({ case ((lemma, sense), targetCounts) => (lemma, computeMatchingScore(targetCounts))})
                .reduceByKey({ case ((correct1, total1), (correct2, total2)) => (correct1 + correct2, total1 + total2)})

            targetsPerSenseResults
                .map({ case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
                .saveAsTextFile(outputFile + "/TargetsPerSense__Results")

            targetsPerSenseResults
                .map({ case (lemma, (correct, total)) => ("TOTAL", (correct, total))})
                .reduceByKey({ case ((correct1, total1), (correct2, total2)) => (correct1 + correct2, total1 + total2)})
                .map({ case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
                .saveAsTextFile(outputFile + "/TargetsPerSense__ResultsAggregated")



            // TARGET -> MOST FREQ. SENSE
            val sensesPerTarget = senseTargetCounts
                .map({ case ((lemma, target, sense), count) => ((lemma, target), (sense, count))})
                .groupByKey()
                .map({ case ((lemma, target), senseCounts) => ((lemma, target), senseCounts.toArray.sortBy(_._2).reverse)})

            sensesPerTarget
                .map({ case ((lemma, target), senseCounts) => lemma + "\t" + target + "\t" + senseCounts.map(senseCount => senseCount._1 + ":" + senseCount._2).mkString("  ")})
                .saveAsTextFile(outputFile + "/SensesPerTarget")

            val sensesPerTargetResults = sensesPerTarget
                .map({ case ((lemma, target), senseCounts) => (lemma, computeMatchingScore(senseCounts))})
                .reduceByKey({ case ((correct1, total1), (correct2, total2)) => (correct1 + correct2, total1 + total2)})

            sensesPerTargetResults
                .map({ case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
                .saveAsTextFile(outputFile + "/SensesPerTarget__Results")

            sensesPerTargetResults
                .map({ case (lemma, (correct, total)) => ("TOTAL", (correct, total))})
                .reduceByKey({ case ((correct1, total1), (correct2, total2)) => (correct1 + correct2, total1 + total2)})
                .map({ case (lemma, (correct, total)) => lemma + "\t" + correct.toDouble / total + "\t" + correct + "/" + total})
                .saveAsTextFile(outputFile + "/SensesPerTarget__ResultsAggregated")
        }
    }
}

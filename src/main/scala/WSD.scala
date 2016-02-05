import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try


object WSDMode extends Enumeration {
    type WSDMode = Value
    val Product, Average = Value

    val DEFAULT = WSDMode.Product

    def fromString(str:String) = {
        val res1 = Try(WSDMode.withName(str))
        if (!res1.isSuccess) {
            val res2 = Try(WSDMode.withName(str(0).isUpper + str.substring(1).toLowerCase()))
            if (!res2.isSuccess) {
                DEFAULT
            } else {
                res2.getOrElse(DEFAULT)
            }
        } else {
            res1.getOrElse(DEFAULT)
        }
    }
}


object WSD {
    val WSD_MODE = WSDMode.Product
    val USE_PRIOR_PROB = true
    val PRIOR_PROB = 0.00001
    val MAX_FEATURE_NUM = 1000000

    val _stopwords = Util.getStopwords()

    def filterFeatureProb(word:String, featureValues:String): (String, Double) = {
        val featureArr = Util.splitLastN(featureValues, Const.SCORE_SEP, 2)
        val feature = featureArr(0)
        if (featureArr.length != 2 || feature.equals(word))
            return null
        val prob = featureArr(1).toDouble

        (feature, prob)
    }

    def loadFeatureProbs(word:String, features:Array[String], maxFeaturesNum:Int): Map[String, Double] = {
        val res = features
            .map(featureValues => filterFeatureProb(word, featureValues))
            .filter(_ != null)
            .take(maxFeaturesNum)
        res.toMap
    }

    def postprocessContext(contextFeatures:Set[String]) = {
        contextFeatures
            .map(feature => feature.toLowerCase())
            .filter(feature => !_stopwords.contains(feature))
            .map(feature => feature.replaceAll("@@","@"))
    }

    def chooseSense(contextFeaturesRaw:Set[String], coocFeatures:Map[Int, (Double, Int, Map[String, Double])], depFeatures:Map[Int, (Double, Int, Map[String, Double])], priorFeatureProb:Double, wsdMode:WSDMode.WSDMode, usePriorProbs:Boolean) = {

        // Initialisation
        val contextFeatures = postprocessContext(contextFeaturesRaw)
        val senseProbs = collection.mutable.Map[Int, Double]()
        val senses = if (depFeatures != null) coocFeatures.keys.toSet.intersect(depFeatures.keys.toSet) else coocFeatures.keys

        // Calculate prior probs
        var sumSenseCount = 0.0
        for (sense <- senses) {
            sumSenseCount += coocFeatures(sense)._1
        }
        for (sense <- senses) {
            if (usePriorProbs) {
                senseProbs(sense) = math.log(coocFeatures(sense)._1 / sumSenseCount)
            } else {
                senseProbs(sense) = 0
            }
        }

        // s* = argmax_s p(s|f1..fn) = argmax_s p(s) * p(f1|s) * .. * p(fn|s)
        var matchedFeatures = new collection.mutable.ListBuffer[String]()
        for (sense <- senses) {
            val coocFeaturesProbs = if (coocFeatures != null) coocFeatures(sense)._3 else Map[String, Double]()
            val depFeaturesProbs = if (depFeatures != null) depFeatures(sense)._3 else Map[String, Double]()
            for (feature <- contextFeatures) {
                var featureProb = priorFeatureProb // Smoothing for previously unseen features; each feature must have the same number of feature probs for all senses.
                if (coocFeaturesProbs.contains(feature)) {
                    featureProb += coocFeaturesProbs(feature)
                    matchedFeatures += feature
                } else if (depFeaturesProbs.contains(feature)) {
                    featureProb += depFeaturesProbs(feature)
                    matchedFeatures += feature
                }
                senseProbs(sense) += math.log(featureProb)
            }
        }

        // return the most probable sense
        val bestSense = senseProbs.toList.sortBy(_._2).last
        "%d\t%.3f\t%.3f\t%s".format(bestSense._1, bestSense._2, bestSense._2/matchedFeatures.size, matchedFeatures.toSet.mkString(Const.LIST_SEP))
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

    def symmetrizeDeps(depFeatures:Array[String]) = {
        var depFeaturesSymmetrized = new collection.mutable.ListBuffer[String]()
        for (feature <- depFeatures) {
            val (depType, srcWord, dstWord) = Util.parseDep(feature)

            if (depType != "?") {
                depFeaturesSymmetrized += feature
                depFeaturesSymmetrized += depType + "(" + dstWord + "," + srcWord + ")"
            }
        }
        depFeaturesSymmetrized.toArray
    }

    def main(args: Array[String]) {
        if (args.size < 4) {
            println("Usage: WSD clusters-with-coocs clusters-with-deps contexts output [prob-smoothing] [wsd-mode] [use-prior-probs] [max-num-features]")
            return
        }

        val conf = new SparkConf().setAppName("WSDEvaluation")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        val clusterCoocsPath = args(0)
        val clusterDepsPath = args(1)
        val contextsPath = args(2)
        val outputPath = args(3)
        val alpha = if (args.length > 4) args(4).toDouble else PRIOR_PROB
        val wsdMode = if (args.length > 5) WSDMode.fromString(args(5)) else WSD_MODE
        val usePriorProbs = if (args.length > 6) args(6).toLowerCase().equals("true") else USE_PRIOR_PROB
        val maxNumFeatures = if (args.length > 7) args(7).toInt else MAX_FEATURE_NUM

        println("Senses with cooc features: " + clusterCoocsPath)
        println("Senses with dependency features: " + clusterDepsPath)
        println("Contexts: " + contextsPath)
        println("Output: " + outputPath)
        println("Prob.smoothing: " + alpha)
        println("WSD mode: " + wsdMode)
        println("Use prior probs.: " + usePriorProbs)
        println("Max feature num: " + maxNumFeatures)

        if (!Util.exists(clusterCoocsPath) && !Util.exists(clusterDepsPath)) {
            println("Error: either coocs or dependency features must exist.")
            return
        }

        Util.delete(outputPath)

        val contexts = sc.textFile(contextsPath)
              .map(line => line.split("\t", -1)) // -1 means "do not drop empty entries"
              .map{case Array(lemma, instanceId, wordFeatures, depFeatures) => (lemma, instanceId, wordFeatures.split(Const.LIST_SEP), depFeatures.split(Const.LIST_SEP))}
              .zipWithIndex()
              .map{case ((lemma, instanceId, wordFeatures, depFeatures), indexId) => (lemma, (indexId, instanceId, wordFeatures ++ symmetrizeDeps(depFeatures)))}
              .cache()

        // Load features in the format: (lemma, (sense -> (feature -> prob)))
        var clustersWithCoocs:RDD[(String, Map[Int, (Double, Int, Map[String, Double])])] = null
        if (Util.exists(clusterCoocsPath)) {
            clustersWithCoocs = sc
              .textFile(clusterCoocsPath)
              .map(line => line.split("\t"))
              .map { case Array(word, senseId, _, senseCount, cluster, features) => (word, senseId.toInt, senseCount.toDouble, cluster.split(Const.LIST_SEP), features.split(Const.LIST_SEP)) }
              .map { case (word, senseId, senseCount, cluster, features) => (word, (senseId, (senseCount, cluster.size, loadFeatureProbs(word, features, maxNumFeatures)))) }
              .groupByKey()
              .mapValues(clusters => clusters.toMap)

            Util.delete(outputPath + ".coocs")
            clustersWithCoocs.saveAsTextFile(outputPath + ".coocs")
        }

        var clustersWithDeps:RDD[(String, Map[Int, (Double, Int, Map[String, Double])])] = null
        if (Util.exists(clusterDepsPath)) {
            clustersWithDeps = sc
              .textFile(clusterDepsPath)  // (lemma, (sense -> (feature -> prob)))
              .map(line => line.split("\t"))
              .map{ case Array(word, senseId, _, senseCount, cluster, features) => (word, senseId.toInt, senseCount.toDouble, cluster.split(Const.LIST_SEP), features.split(Const.LIST_SEP))}
              .map{ case (word, senseId, senseCount, cluster, features) => (word, (senseId, (senseCount, cluster.size, loadFeatureProbs(word, features, maxNumFeatures))))}
              .groupByKey()
              .mapValues(clusters => clusters.toMap)

            Util.delete(outputPath + ".deps")
            clustersWithDeps.saveAsTextFile(outputPath + ".deps")
        }

        // Classify contexts
        var result: RDD[(String, Long, String, String, Array[String])] = null
        if (clustersWithDeps != null && clusterCoocsPath != null) {
            result = contexts
                .join(clustersWithCoocs)
                .join(clustersWithDeps)
                .map{case (lemma, (((sentId, target, tokens), coocFeatures), depFeatures)) => (lemma, sentId, target, chooseSense(tokens.toSet, coocFeatures, depFeatures, alpha, wsdMode, usePriorProbs), tokens)}
        } else if (clusterCoocsPath != null) {
            result = contexts
                .join(clustersWithCoocs)
                .map{case (lemma, ((sentId, target, tokens), coocFeatures)) => (lemma, sentId, target, chooseSense(tokens.toSet, coocFeatures, null, alpha, wsdMode, usePriorProbs), tokens)}
        } else if (clusterDepsPath != null) {
            result = contexts
              .join(clustersWithCoocs)
              .map{case (lemma, ((sentId, target, tokens), depsFeatures)) => (lemma, sentId, target, chooseSense(tokens.toSet, null, depsFeatures, alpha, wsdMode, usePriorProbs), tokens)}
        }

        result
            .map({case (lemma, instanceId, target, senseId, tokens) => lemma + "\t" + target + "\t" + senseId + "\t" + tokens.mkString(Const.LIST_SEP)})
            .saveAsTextFile(outputPath)
    }
}

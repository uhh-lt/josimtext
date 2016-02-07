import _root_.WSDFeatures.WSDFeatures
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.util.Try


object WSD {
    val WSD_MODE = WSDFeatures.DEFAULT
    val USE_PRIOR_PROB = true
    val PRIOR_PROB = 0.00001
    val MAX_FEATURE_NUM = 1000000
    val PARTITIONS_NUM = 16
    val ALL_HOLING_FEATURES = false
    val STRONG_FEATURES_BOOST = 3  // boost for strong sparse features (target dependencies and sense cluster words)

    val _stopwords = Util.getStopwords()

    def getContextFeatures(word_features: String, target_holing_features: String, holing_features:String, featuresMode:WSDFeatures): Array[String] = {
        word_features.split(Const.LIST_SEP) ++ target_holing_features.split(Const.LIST_SEP) ++ symmetrizeDeps(target_holing_features.split(Const.LIST_SEP))
    }

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

    def chooseSense(contextFeaturesRaw:Set[String], coocFeatures:Map[Int, (Double, Int, Map[String, Double])], depFeatures:Map[Int, (Double, Int, Map[String, Double])], priorFeatureProb:Double, wsdMode:WSDFeatures.WSDFeatures, usePriorProbs:Boolean) = {
        // Initialisation
        val contextFeatures = postprocessContext(contextFeaturesRaw)
        val senseProbs = collection.mutable.Map[Int, Double]()
        var someFeatures: Map[Int, (Double, Int, Map[String, Double])] = null
        var senses:Iterable[Int] = null

        if (coocFeatures != null && depFeatures != null){
            coocFeatures.keys.toSet.intersect(depFeatures.keys.toSet)
            someFeatures = coocFeatures
        } else if (coocFeatures != null) {
            senses = coocFeatures.keys
            someFeatures = coocFeatures
        } else if (depFeatures != null) {
            senses = depFeatures.keys
            someFeatures = depFeatures
        } else {
            senses = Set[Int]()
            someFeatures = Map[Int, (Double, Int, Map[String, Double])]()
        }

        // Calculate prior probs
        var sumSenseCount = 0.0

        for (sense <- senses) {
            sumSenseCount +=  someFeatures(sense)._1
        }
        for (sense <- senses) {
            if (usePriorProbs) {
                senseProbs(sense) = math.log(someFeatures(sense)._1 / sumSenseCount)
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
        val bestSenseId = bestSense._1
        val bestSenseConf = bestSense._2
        val bestSenseConfNorm = if (matchedFeatures.size > 0) bestSense._2/matchedFeatures.size else bestSense._2
        val usedFeatures = matchedFeatures.toSet.mkString(Const.LIST_SEP)

        (bestSenseId, bestSenseConf, bestSenseConfNorm, usedFeatures)
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
            println("Usage: WSD clusters-with-coocs clusters-with-deps lex-sample-dataset output [prob-smoothing] [wsd-mode] [use-prior-probs] [max-num-features] [partitions-num]")
            return
        }

        val conf = new SparkConf().setAppName("WSDEvaluation")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        val clusterCoocsPath = args(0)
        val clusterDepsPath = args(1)
        val contextsPath = args(2)
        val outputPath = args(3)
        val priorProb = if (args.length > 4) args(4).toDouble else PRIOR_PROB
        val wsdMode = if (args.length > 5) WSDFeatures.fromString(args(5)) else WSD_MODE
        val usePriorProb = if (args.length > 6) args(6).toLowerCase().equals("true") else USE_PRIOR_PROB
        val maxNumFeatures = if (args.length > 7) args(7).toInt else MAX_FEATURE_NUM
        val partitionsNum = if (args.length > 8) args(8).toInt else MAX_FEATURE_NUM

        println("Senses with cooc features: " + clusterCoocsPath)
        println("Senses with dependency features: " + clusterDepsPath)
        println("Lexical sample dataset: " + contextsPath)
        println("Output: " + outputPath)
        println("Prob.smoothing: " + priorProb)
        println("WSD mode: " + wsdMode)
        println("Use prior probs.: " + usePriorProb)
        println("Max feature num: " + maxNumFeatures)
        println("Number of partitions of sense features: " + partitionsNum)


        if (!Util.exists(clusterCoocsPath) && !Util.exists(clusterDepsPath)) {
            println("Error: either coocs or dependency features must exist.")
            return
        } else if (!Util.exists(clusterCoocsPath)) {
            println("Warning: coocs features not available. Using only deps features.")
        } else if (!Util.exists(clusterDepsPath)) {
            println("Warning: deps features not available. Using only coocs features.")
        }
        Util.delete(outputPath)

        // target, (dataset, features)
        // dataset: context_id	target	target_pos	target_position	gold_sense_ids	predict_sense_ids	golden_related	predict_related	context word_features	holing_features	target_holing_features
        val lexSample: RDD[(String, (Array[String], (String, String, String, String, String, String, String, String, String, String, String, String)))] = sc.textFile(contextsPath)  // target,
          .map(line => line.split("\t", -1))
          .map{case Array(context_id,	target,	target_pos,	target_position, gold_sense_ids, predict_sense_ids,	golden_related,	predict_related, context, word_features, holing_features,	target_holing_features) =>
              (target, (
                  getContextFeatures(word_features, holing_features, target_holing_features, wsdMode),
                  (context_id, target,	target_pos,	target_position, gold_sense_ids, predict_sense_ids,	golden_related,	predict_related, context, word_features, holing_features,	target_holing_features)))
              //case _ => ("", (Array[String](), ("", "", "", "", "", "", "", "", "", "", "", "")))
          }
          .cache()

        // Load features in the format: (lemma, (sense -> (feature -> prob)))
        var clustersWithCoocs:RDD[(String, Map[Int, (Double, Int, Map[String, Double])])] = null
        if (Util.exists(clusterCoocsPath)) {
            clustersWithCoocs = sc
                .textFile(clusterCoocsPath)
                .map(line => line.split("\t"))
                .map{ case Array(word, senseId, _, senseCount, cluster, features) => (word, senseId.toInt, senseCount.toDouble, cluster.split(Const.LIST_SEP), features.split(Const.LIST_SEP)) }
                .map{ case (word, senseId, senseCount, cluster, features) => (word, (senseId, (senseCount, cluster.size, loadFeatureProbs(word, features, maxNumFeatures)))) }
                .groupByKey()
                .mapValues(clusters => clusters.toMap)
                .partitionBy(new HashPartitioner(partitionsNum))
                .persist()
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
                .partitionBy(new HashPartitioner(partitionsNum))
                .persist()
        }

        // Classify contexts
        var result: RDD[((String, String, String, String, String, String, String, String, String, String, String, String), (Int, Double, Double, String))] = null
        if (clustersWithDeps != null && clustersWithCoocs != null) {
            result = lexSample
              .join(clustersWithCoocs)
              .join(clustersWithDeps)
              .map{case (target, (((tokens, dataset), coocFeatures), depFeatures)) => (dataset, chooseSense(tokens.toSet, coocFeatures, depFeatures, priorProb, wsdMode, usePriorProb))}
        } else if (clustersWithCoocs != null) {
            result = lexSample
              .join(clustersWithCoocs)
              .map{case (target, ((tokens, dataset), coocFeatures)) => (dataset, chooseSense(tokens.toSet, coocFeatures, null, priorProb, wsdMode, usePriorProb))}
        } else if (clustersWithDeps != null) {
            result = lexSample
              .join(clustersWithDeps)
              .map{case (target, ((tokens, dataset), depsFeatures)) => (dataset, chooseSense(tokens.toSet, null, depsFeatures, priorProb, wsdMode, usePriorProb))}
        }

        result
            .map({case ((context_id,	target,	target_pos,	target_position, gold_sense_ids, predict_sense_ids,	golden_related,	predict_related, context, word_features, holing_features,	target_holing_features),(bestSenseId, bestSenseConf, bestSenseConfNorm, usedFeatures)) =>
                context_id + "\t" +	target + "\t" +	target_pos + "\t" +	target_position + "\t" +	gold_sense_ids + "\t" +	bestSenseId + "\t" +	golden_related + "\t" +
                    predict_related + "\t" + context + "\t" +	word_features + "\t" +	holing_features + "\t" + target_holing_features + "\t" + bestSenseConf + "\t" +	bestSenseConfNorm + "\t" + usedFeatures })
            .saveAsTextFile(outputPath)
    }


}

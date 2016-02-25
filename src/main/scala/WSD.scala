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
    val DEPS_TARGET_FEATURES_BOOST = 3  // boost for strong sparse features
    val SYMMETRIZE_DEPS = false
    val DEFAULT_SENSE_ID = 1
    val DEFAULT_FEATURENUM_CONF = "0"
    val DEBUG = false

    val _stopwords = Util.getStopwords()

    def getContextFeatures(wordFeaturesStr: String, depstargetFeaturesStr: String, depsallFeaturesStr:String, featuresMode:WSDFeatures.Value): Array[String] = {

        val wordFeatures =
            if (!WSDFeatures.wordsNeeded(featuresMode)) Array[String]()
            else wordFeaturesStr.split(Const.LIST_SEP)

        val depstargetFeatures =
            if (!WSDFeatures.depstargetNeeded(featuresMode)){
                Array[String]()
            } else if (SYMMETRIZE_DEPS) {
                val singleFeatures = symmetrizeDeps(depstargetFeaturesStr.split(Const.LIST_SEP))
                var boostedFeatures = List[String]()
                for (i <- 1 to DEPS_TARGET_FEATURES_BOOST) boostedFeatures = boostedFeatures ++ singleFeatures
                boostedFeatures.toArray
            } else {
                depstargetFeaturesStr.split(Const.LIST_SEP)
            }

        val depsallFeatures =
            if (!WSDFeatures.depsallNeeded(featuresMode)) Array[String]()
            else if (SYMMETRIZE_DEPS) symmetrizeDeps(depsallFeaturesStr.split(Const.LIST_SEP))
            else depsallFeaturesStr.split(Const.LIST_SEP)

        if (DEBUG) {
            println("word (%d): %s".format(wordFeatures.size, wordFeatures.mkString(",")))
            println("target deps (%d): %s".format(depstargetFeatures.size, depstargetFeatures.mkString(",")))
            println("all deps (%d): %s".format(depsallFeatures.size, depsallFeatures.mkString(",")))
            println(">>> " + (wordFeatures ++ depstargetFeatures ++ depsallFeatures).size + "\n")
        }

        wordFeatures ++ depstargetFeatures ++ depsallFeatures
    }

    def filterFeatureProb(word:String, featureValues:String): (String, Double) = {
        val featureArr = Util.splitLastN(featureValues, Const.SCORE_SEP, 2)
        val feature = featureArr(0)
        if (featureArr.length != 2 || feature.equals(word))
            return null
        val prob = featureArr(1).toDouble

        (feature, prob)
    }

    def parseFeatures(word:String, featuresStr:String, maxFeaturesNum:Int): Map[String, Double] = {
        val res = featuresStr
            .split(Const.LIST_SEP)
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

    def predictSense(contextFeaturesRaw:Set[String], clusterFeatures:Map[Int, Map[String, Double]],
            coocFeatures:Map[Int, (Double, Int, Map[String, Double])], depFeatures:Map[Int, (Double, Int, Map[String, Double])],
            priorFeatureProb:Double, usePriorProbs:Boolean) = {

        if (DEBUG) {
            println("clusters available: " + (clusterFeatures != null).toString)
            println("coocs available: " + (coocFeatures != null).toString)
            println("deps available: " + (depFeatures != null).toString)
        }

        // Initialisation
        val contextFeatures = postprocessContext(contextFeaturesRaw)
        val senseProbs = collection.mutable.Map[Int, Double]()
        var someFeatures: Map[Int, (Double, Int, Map[String, Double])] = null
        var senses:Iterable[Int] = null

        if (coocFeatures != null && depFeatures != null){
            senses = coocFeatures.keys.toSet.intersect(depFeatures.keys.toSet)
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
        var usedFeatures = new collection.mutable.ListBuffer[String]()
        var allFeatures = new collection.mutable.ListBuffer[String]()
        for (sense <- senses) {
            val coocFeaturesProbs = if (coocFeatures != null) coocFeatures(sense)._3 else Map[String, Double]()
            val depFeaturesProbs = if (depFeatures != null) depFeatures(sense)._3 else Map[String, Double]()
            for (feature <- contextFeatures) {
                val featureProb =
                    if (coocFeaturesProbs.contains(feature)) {
                        usedFeatures += feature
                        coocFeaturesProbs(feature) + priorFeatureProb
                    } else if (depFeaturesProbs.contains(feature)) {
                        usedFeatures += feature

                        depFeaturesProbs(feature) + priorFeatureProb
                    } else {
                        priorFeatureProb // Smoothing for previously unseen features; each feature must have the same number of feature probs for all senses.
                    }
                allFeatures += "%d:%s:%.6f".format(sense, feature, featureProb)
                senseProbs(sense) += math.log(featureProb)
            }
        }

        // return the most probable sense
        if (senseProbs.size > 0) {
            val senseProbsSorted = senseProbs.toList.sortBy(_._2)
            val bestSense = senseProbsSorted.last
            val bestSenseId = bestSense._1
            val bestSenseConfStr = bestSense._2.toString
            val bestSenseConfNormStr = if (usedFeatures.size > 0) (bestSense._2 / usedFeatures.size).toString else bestSense._2.toString
            val usedFeaturesStr = usedFeatures.toSet.mkString(Const.LIST_SEP)
            val allFeaturesStr = allFeatures.mkString(Const.LIST_SEP)

            val worstSense = senseProbsSorted(0)
            val rangeConfStr = "%.3f".format(bestSense._2 - worstSense._2)
            val featurenumConfStr = usedFeatures.size.toString

            (bestSenseId, rangeConfStr, featurenumConfStr, bestSenseConfStr + "\t" + bestSenseConfNormStr + "\t" + usedFeaturesStr + "\t" + allFeaturesStr)
        } else {
            (DEFAULT_SENSE_ID, DEFAULT_FEATURENUM_CONF, DEFAULT_FEATURENUM_CONF, "\t")
        }
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

        val clustersPath = args(0)
        val clusterCoocsPath = args(1)
        val clusterDepsPath = args(2)
        val contextsPath = args(3)
        val outputPath = args(4)
        val priorProb = if (args.length > 5) args(5).toDouble else PRIOR_PROB
        val wsdMode = if (args.length > 6) WSDFeatures.fromString(args(6)) else WSD_MODE
        val usePriorProb = if (args.length > 7) args(7).toLowerCase().equals("true") else USE_PRIOR_PROB
        val maxNumFeatures = if (args.length > 8) args(8).toInt else MAX_FEATURE_NUM
        val partitionsNum = if (args.length > 9) args(9).toInt else PARTITIONS_NUM

        println("Senses: " + clustersPath)
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
          .map{ case Array(context_id,	target,	target_pos,	target_position, gold_sense_ids, predict_sense_ids,	golden_related,	predict_related, context, word_features, holing_features,	target_holing_features) =>
              (target, (
                  getContextFeatures(word_features, target_holing_features, holing_features, wsdMode),
                  (context_id, target,	target_pos,	target_position, gold_sense_ids, predict_sense_ids,	golden_related,	predict_related, context, word_features, holing_features,	target_holing_features)))
              case _ => ("", (Array[String](), ("", "", "", "", "", "", "", "", "", "", "", "")))
          }
          .cache()

        // Load features in the format: (lemma, (sense -> (feature -> prob)))
        var clusterFeatures:RDD[(String, Map[Int, Map[String, Double]])] = null
        if (WSDFeatures.clustersNeeded(wsdMode) && Util.exists(clustersPath)) {
            clusterFeatures = sc
              .textFile(clustersPath)  // (lemma, (sense -> (feature -> prob)))
              .map{ line => line.split("\t") }
              .map{ case Array(word, senseId, _, cluster) => (word, (senseId.toInt, (parseFeatures(word, cluster, maxNumFeatures)))) }
              .groupByKey()
              .mapValues(clusters => clusters.toMap)
              .partitionBy(new HashPartitioner(partitionsNum))
              .persist()
        }

        var coocFeatures:RDD[(String, Map[Int, (Double, Int, Map[String, Double])])] = null
        if (WSDFeatures.coocsNeeded(wsdMode) && Util.exists(clusterCoocsPath)) {
            coocFeatures = sc
                .textFile(clusterCoocsPath)
                .map{ line => line.split("\t") }
                .map{ case Array(word, senseId, _, senseCount, cluster, features) => (word, senseId.toInt, senseCount.toDouble, cluster.split(Const.LIST_SEP), features) }
                .map{ case (word, senseId, senseCount, cluster, features) => (word, (senseId, (senseCount, cluster.size, parseFeatures(word, features, maxNumFeatures)))) }
                .groupByKey()
                .mapValues(clusters => clusters.toMap)
                .partitionBy(new HashPartitioner(partitionsNum))
                .persist()
        }

        var depFeatures:RDD[(String, Map[Int, (Double, Int, Map[String, Double])])] = null
        if (WSDFeatures.depsNeeded(wsdMode) && Util.exists(clusterDepsPath)) {
            depFeatures = sc
                .textFile(clusterDepsPath)  // (lemma, (sense -> (feature -> prob)))
                .map(line => line.split("\t"))
                .map{ case Array(word, senseId, _, senseCount, cluster, features) => (word, senseId.toInt, senseCount.toDouble, cluster.split(Const.LIST_SEP), features) }
                .map{ case (word, senseId, senseCount, cluster, features) => (word, (senseId, (senseCount, cluster.size, parseFeatures(word, features, maxNumFeatures)))) }
                .groupByKey()
                .mapValues(clusters => clusters.toMap)
                .partitionBy(new HashPartitioner(partitionsNum))
                .persist()
        }

        // Classify contexts
        var result: RDD[((String, String, String, String, String, String, String, String, String, String, String, String), (Int, String, String, String))] = null
        if (clusterFeatures != null && coocFeatures != null && depFeatures != null) {
            result = lexSample
                .join(coocFeatures)
                .join(depFeatures)
                .join(clusterFeatures)
                .map{ case (target, ((((tokens, dataset), coocs), deps),clusters)) => (dataset, predictSense(tokens.toSet, clusters, coocs, deps, priorProb, usePriorProb)) }
        } else if (clusterFeatures == null && coocFeatures != null && depFeatures != null) {
            result = lexSample
                .join(coocFeatures)
                .join(depFeatures)
                .map{ case (target, (((tokens, dataset), coocs), deps)) => (dataset, predictSense(tokens.toSet, null, coocs, deps, priorProb, usePriorProb)) }
        } else if (clusterFeatures != null && coocFeatures != null && depFeatures == null) {
            result = lexSample
              .join(coocFeatures)
              .join(clusterFeatures)
              .map{ case (target, (((tokens, dataset), coocs), clusters)) => (dataset, predictSense(tokens.toSet, clusters, coocs, null, priorProb, usePriorProb)) }
        } else if (clusterFeatures != null && coocFeatures == null && depFeatures != null) {
            result = lexSample
              .join(depFeatures)
              .join(clusterFeatures)
              .map{ case (target, (((tokens, dataset), deps), clusters)) => (dataset, predictSense(tokens.toSet, clusters, null, deps, priorProb, usePriorProb)) }
        } else if (clusterFeatures == null && coocFeatures != null && depFeatures == null) {
            result = lexSample
                .join(coocFeatures)
                .map{ case (target, ((tokens, dataset), coocs)) => (dataset, predictSense(tokens.toSet, null, coocs, null, priorProb, usePriorProb)) }
        } else if (clusterFeatures == null && coocFeatures == null && depFeatures != null) {
            result = lexSample
                .join(depFeatures)
                .map{ case (target, ((tokens, dataset), deps)) => (dataset, predictSense(tokens.toSet, null, null, deps, priorProb, usePriorProb)) }
        } else if (clusterFeatures != null && coocFeatures == null && depFeatures == null) {
            result = lexSample
                .join(clusterFeatures)
                .map{ case (target, ((tokens, dataset), clusters)) => (dataset, predictSense(tokens.toSet, clusters, null, null, priorProb, usePriorProb)) }
        } else {
            // normally shall never happen, prevented by checks above
        }

        result
            .map{ case ((context_id,	target,	target_pos,	target_position, gold_sense_ids, predict_sense_ids,	golden_related,	predict_related, context, word_features, holing_features,	target_holing_features),(bestSenseId, bestSenseConf, bestSenseConfNorm, usedFeatures)) =>
                context_id + "\t" +	target + "\t" +	target_pos + "\t" +	target_position + "\t" +	gold_sense_ids + "\t" +	bestSenseId + "\t" +	golden_related + "\t" +
                    predict_related + "\t" + context + "\t" +	word_features + "\t" +	holing_features + "\t" + target_holing_features + "\t" + bestSenseConf + "\t" +	bestSenseConfNorm + "\t" + usedFeatures }
            .saveAsTextFile(outputPath)
    }


}

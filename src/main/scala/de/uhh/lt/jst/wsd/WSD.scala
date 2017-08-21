package de.uhh.lt.jst.wsd

import de.uhh.lt.jst.utils.{Const, KryoDiskSerializer, Util}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by sasha on 13/06/17.
  */
object WSD {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val WSD_MODE = WSDFeatures.DepstargetCoocsClustersTrigramstarget
  val USE_PRIOR_PROB = true
  val MAX_FEATURE_NUM = 1000000
  val PARTITIONS_NUM = 16
  val TARGET_FEATURES_BOOST = 3
  // boost strong but sparse target features
  val SYMMETRIZE_DEPS = true
  val DEBUG = false
  val FEATURES_POSTFIX = "-features"
  val SAVE_FEATURES = true

  val _stopwords = Util.getStopwords()

  /**
    * Parses featureScore pair e.g. 'f:0.99' and returns a tuple f->0.99
    **/
  def parseFeature(word: String, featureScoreStr: String): (String, Double) = {
    val featureArr = Util.splitLastN(featureScoreStr, Const.SCORE_SEP, 2)
    val feature = featureArr(0)
    if (featureArr.length != 2 || feature.equals(word))
      return (Const.NO_FEATURE_LABEL, Const.NO_FEATURE_CONF)
    val prob = featureArr(1).toDouble

    (feature, prob)
  }

  /**
    * Returns a map features->scores from a string representations e.g. "f:0.99  z:0.88".
    **/
  def parseFeatures(word: String, featuresStr: String, maxFeaturesNum: Int = MAX_FEATURE_NUM): Map[String, Double] = {

    val res = featuresStr
      .split(Const.LIST_SEP)
      .map(featureValues => parseFeature(word, featureValues))
      .filter(_ != (Const.NO_FEATURE_LABEL, Const.NO_FEATURE_CONF))
      .take(maxFeaturesNum)
    res.toMap
  }

  def getHolingFeatures(holingFeaturesStr: String, symmetrize: Boolean) = {
    val holingFeatures = holingFeaturesStr.split(Const.LIST_SEP)
    val holingDeps = holingFeatures.filter { case f => f.contains("(@,") || f.contains(",@)") }
    val holingDepsSym = if (symmetrize) symmetrizeDeps(holingDeps) else holingDeps
    val holingTrigrams = holingFeatures.filter { case f => f.contains("_@_") }

    (holingDepsSym.toList, holingTrigrams.toList)
  }

  def boostFeatures(features: List[String], boostFactor: Int) = {
    var res = List[String]()
    for (i <- 1 to boostFactor) res = res ++ features
    res
  }

  def getContextFeatures(wordFeaturesStr: String, holingTargetFeaturesStr: String, holingAllFeaturesStr: String, featuresMode: WSDFeatures.Value): Array[String] = {
    val wordFeatures = wordFeaturesStr.split(Const.LIST_SEP)
    val (depsTarget, trigramsTarget) = getHolingFeatures(holingTargetFeaturesStr, SYMMETRIZE_DEPS)
    val depsTargetBoosted = boostFeatures(depsTarget, TARGET_FEATURES_BOOST)
    val trigramsTargetBoosted = boostFeatures(trigramsTarget, TARGET_FEATURES_BOOST)
    val (depsAll, trigramsAll) = getHolingFeatures(holingAllFeaturesStr, SYMMETRIZE_DEPS)

    var res = new collection.mutable.ListBuffer[String]()
    if (WSDFeatures.wordsNeeded(featuresMode)) res ++= wordFeatures
    if (WSDFeatures.depstargetNeeded(featuresMode)) res ++= depsTargetBoosted
    if (WSDFeatures.trigramstargetNeeded(featuresMode)) res ++= trigramsTargetBoosted
    if (WSDFeatures.depsallNeeded(featuresMode)) res ++= depsAll
    if (WSDFeatures.trigramsallNeeded(featuresMode)) res ++= trigramsAll
    res.toArray
  }

  def postprocessContext(contextFeatures: Array[String]) = {
    contextFeatures
      .map(feature => feature.toLowerCase())
      .filter(feature => !_stopwords.contains(feature))
      .filter(feature => feature.length > 1)
      .filter(feature => !Util.isNumber(feature))
      .map(feature => feature.replaceAll("@@", "@"))
  }

  def predict(contextFeaturesRaw: Array[String],
              inventory: Map[Int, Map[String, Double]],
              clusterFeatures: Map[Int, Map[String, Double]],
              coocFeatures: Map[Int, Map[String, Double]],
              depFeatures: Map[Int, Map[String, Double]],
              trigramFeatures: Map[Int, Map[String, Double]],
              usePriors: Boolean,
              rejectNoFeatures: Boolean = true): Prediction = {

    // Initialisation
    val contextFeatures = postprocessContext(contextFeaturesRaw)
    val senseProbs = collection.mutable.Map[Int, Double]()

    // Prior sense probabilities
    var sensePriors = new collection.mutable.ListBuffer[String]()
    val allClusterWordsNum = inventory
      .map { case (senseId, senseCluster) => senseCluster.size }
      .sum
      .toDouble

    for (sense <- inventory.keys) {
      if (usePriors) {
        val sensePrior = math.log(inventory(sense).size / allClusterWordsNum)
        senseProbs(sense) = sensePrior
        sensePriors += "%d:%.6f".format(sense, sensePrior)
      }
    }

    // Conditional probabilities of context features
    var usedFeatures = new collection.mutable.ListBuffer[String]()
    var allFeatures = new collection.mutable.ListBuffer[String]()
    for (sense <- inventory.keys) {
      for (feature <- contextFeatures) {
        val featureProb =
          if (clusterFeatures.contains(sense) && clusterFeatures(sense).contains(feature)) {
            usedFeatures += feature
            clusterFeatures(sense).getOrElse(feature, Const.PRIOR_PROB)
          } else if (coocFeatures.contains(sense) && coocFeatures(sense).contains(feature)) {
            usedFeatures += feature
            coocFeatures(sense).getOrElse(feature, Const.PRIOR_PROB)
          } else if (depFeatures.contains(sense) && depFeatures(sense).contains(feature)) {
            usedFeatures += feature
            depFeatures(sense).getOrElse(feature, Const.PRIOR_PROB)
          } else if (trigramFeatures.contains(sense) && trigramFeatures(sense).contains(feature)) {
            usedFeatures += feature
            trigramFeatures(sense).getOrElse(feature, Const.PRIOR_PROB)
          } else {
            Const.PRIOR_PROB // Smoothing for previously unseen features; each feature must have the same number of feature probs for all senses.
          }
        allFeatures += "%d:%s:%.6f".format(sense, feature, featureProb)
        senseProbs(sense) += (if (featureProb >= Const.PRIOR_PROB) math.log(featureProb) else math.log(Const.PRIOR_PROB))
      }
    }

    // return the most probable sense
    val res = new Prediction()
    if (senseProbs.size > 0) {
      val senseProbsSorted: List[(Int, Double)] = if (usePriors) senseProbs.toList.sortBy(_._2) else Random.shuffle(senseProbs.toList).sortBy(_._2) // to ensure that order doesn't influence choice
      val bestSense = senseProbsSorted.last

      res.confs = senseProbsSorted.map { case (label, score) => s"%s:%.3f".format(label, score) }
      val worstSense = senseProbsSorted(0)
      res.predictConf = math.abs(bestSense._2 - worstSense._2)
      res.usedFeaturesNum = usedFeatures.size
      res.bestConfNorm = if (usedFeatures.size > 0) (bestSense._2 / usedFeatures.size) else bestSense._2
      res.usedFeatures = usedFeatures.toSet
      res.allFeatures = allFeatures.toList
      res.sensePriors = sensePriors.toList
      res.predictRelated = inventory(bestSense._1).map { case (label, score) => s"%s:%.3f".format(label, score) }.toList
    }
    res
  }

  def computeMatchingScore[T](matchingCountsSorted: Array[(T, Int)]): (Int, Int) = {
    val countSum = matchingCountsSorted.map(matchCount => matchCount._2).sum
    // return "highest count" / "sum of all counts"
    (matchingCountsSorted.head._2, countSum)
  }

  def symmetrizeDeps(depFeatures: Array[String]) = {
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

  def loadCols2Features(featuresCols: RDD[(String, Int, String, String)], maxNumFeatures: Int, partitionsNum: Int): RDD[(String, Map[Int, Map[String, Double]])] = {
    featuresCols
      .map { case (word, senseId, cluster, features) => (word, (senseId, (parseFeatures(word, features, maxNumFeatures)))) }
      .groupByKey()
      .mapValues(features => features.toMap)
      .partitionBy(new HashPartitioner(partitionsNum))
      .persist()
  }

  def loadFile2Cols(sc: SparkContext, featuresPath: String): RDD[(String, Int, String, String)] = {
    sc.textFile(featuresPath)
      .map { line => line.split("\t") }
      .map { case Array(word, senseId, cluster, features) => (word, senseId.toInt, cluster, features) case _ => ("?", -1, "", "") }
  }

  def buildFeatures(sc: SparkContext, clustersPath: String, coocsPath: String, depsPath: String, trigramsPath: String,
                    wsdMode: WSDFeatures.Value, maxNumFeatures: Int, partitionsNum: Int):
  RDD[(String, (Map[Int, Map[String, Double]], Map[Int, Map[String, Double]], Map[Int, Map[String, Double]], Map[Int, Map[String, Double]], Map[Int, Map[String, Double]]))] = {

    val featuresPath = clustersPath + FEATURES_POSTFIX
    val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    val featuresExist = fs.exists(new org.apache.hadoop.fs.Path(featuresPath))
    println(s"$featuresPath: $featuresExist")

    if (featuresExist) {
      val loadedFeatures = KryoDiskSerializer.objectFile[(String, (Map[Int, Map[String, Double]], Map[Int, Map[String, Double]], Map[Int, Map[String, Double]], Map[Int, Map[String, Double]], Map[Int, Map[String, Double]]))](sc, featuresPath)
      loadedFeatures
    } else {
      // Load clusters senses and the associated features (required)
      // senses and features are the format: (lemma, (sense -> (feature -> prob)))
      val clustersCols = loadFile2Cols(sc, clustersPath).persist()

      val inventory: RDD[(String, Map[Int, Map[String, Double]])] = clustersCols
        .map { case (word, senseId, cluster, features) => (word, (senseId, (parseFeatures(word, cluster)))) }
        .groupByKey()
        .mapValues(clusters => clusters.toMap)
      println(s"#words (inventory): ${inventory.count()}")

      val noFeatures: RDD[(String, Map[Int, Map[String, Double]])] = clustersCols
        .map { case (word, senseId, cluster, features) => (word, (senseId, (Map[String, Double]()))) }
        .groupByKey()
        .mapValues(clusters => clusters.toMap)
        .persist()
      println(s"#words (no features): ${noFeatures.count()}")

      val clusterFeatures: RDD[(String, Map[Int, Map[String, Double]])] =
        if (WSDFeatures.clustersNeeded(wsdMode)) {
          loadCols2Features(clustersCols, maxNumFeatures, partitionsNum)
        } else {
          noFeatures
        }
      println(s"#words (cluster features): ${clusterFeatures.count()}")

      val coocFeatures =
        if (WSDFeatures.coocsNeeded(wsdMode) && Util.exists(coocsPath)) {
          val cols = loadFile2Cols(sc, coocsPath)
          loadCols2Features(cols, maxNumFeatures, partitionsNum)
        } else {
          if (WSDFeatures.coocsNeeded(wsdMode) && !Util.exists(coocsPath)) println(s"error: not found $coocsPath")
          noFeatures
        }
      println(s"#words (coocs features): ${clusterFeatures.count()}")

      val depsFeatures =
        if (WSDFeatures.depsNeeded(wsdMode) && Util.exists(depsPath)) {
          val cols = loadFile2Cols(sc, depsPath)
          loadCols2Features(cols, maxNumFeatures, partitionsNum)
        } else {
          if (WSDFeatures.depsNeeded(wsdMode) && !Util.exists(depsPath)) println(s"error: not found $depsPath")
          noFeatures
        }
      println(s"#words (deps features): ${depsFeatures.count()}")

      val trigramFeatures =
        if (WSDFeatures.trigramsNeeded(wsdMode) && Util.exists(trigramsPath)) {
          val cols = loadFile2Cols(sc, trigramsPath)
          loadCols2Features(cols, maxNumFeatures, partitionsNum)
        } else {
          if (WSDFeatures.trigramsNeeded(wsdMode) && !Util.exists(trigramsPath)) println(s"error: not found $trigramsPath")
          noFeatures
        }
      println(s"#words (trigrams features): ${trigramFeatures.count()}")

      val newFeatures = inventory
        .join(clusterFeatures)
        .join(coocFeatures)
        .join(depsFeatures)
        .join(trigramFeatures)
        .map { case (target, ((((inventoryT, clustersT), coocsT), depsT), trigramsT)) => (target, (inventoryT, clustersT, coocsT, depsT, trigramsT)) }
      println(s"#features: ${newFeatures.count()}")

      if (SAVE_FEATURES) {
        KryoDiskSerializer.saveAsObjectFile(newFeatures, featuresPath)
        println(s"Saved features: $featuresPath")
      }
      newFeatures
    }
  }

  def run(sc: SparkContext, lexSamplePath: String, outputPath: String, clustersPath: String, coocsPath: String = "",
          depsPath: String = "", trigramsPath: String = "", usePriorProb: Boolean = USE_PRIOR_PROB, wsdMode: WSDFeatures.Value = WSD_MODE,
          maxNumFeatures: Int = MAX_FEATURE_NUM, partitionsNum: Int = PARTITIONS_NUM) = {

    // Load lexical sample
    // target, (dataset, features)
    // dataset: context_id	target	target_pos	target_position	gold_sense_ids	predict_sense_ids	golden_related	predict_related	context word_features	holing_features	target_holing_features
    val lexSample: RDD[(String, (Array[String], (String, String, String, String, String, String, String, String, String, String, String, String)))] = sc.textFile(lexSamplePath)
      .map(line => line.split("\t", -1))
      .map { case Array(context_id, target, target_pos, target_position, gold_sense_ids, predict_sense_ids, golden_related, predict_related, context, word_features, holing_features, target_holing_features) =>
        (target, (
          getContextFeatures(word_features, target_holing_features, holing_features, wsdMode),
          (context_id, target, target_pos, target_position, gold_sense_ids, predict_sense_ids, golden_related, predict_related, context, word_features, holing_features, target_holing_features)))
      case _ => ("", (Array[String](), ("", "", "", "", "", "", "", "", "", "", "", "")))
      }
      .cache()
    println(s"# lexical samples: ${lexSample.count()}")

    val features = buildFeatures(sc, clustersPath, coocsPath, depsPath, trigramsPath, wsdMode, maxNumFeatures, partitionsNum)

    val result = lexSample
      .join(features)
      .map { case (target, ((contextT, dataset), (inventoryT, clustersT, coocsT, depsT, trigramsT))) =>
        (dataset, predict(contextT, inventoryT, clustersT, coocsT, depsT, trigramsT, usePriorProb))
      }

    println(s"#classified lex samples: ${result.count()}")
    Util.delete(outputPath)

    // Save results in the CSV format
    result
      .map { case ((context_id, target, target_pos, target_position, gold_sense_ids, predict_sense_ids, golden_related, predict_related, context, word_features, holing_features, target_holing_features), prediction) =>
        context_id + "\t" +
          target + "\t" +
          target_pos + "\t" +
          target_position + "\t" +
          gold_sense_ids + "\t" +
          prediction.confs.mkString(Const.LIST_SEP) + "\t" +
          golden_related + "\t" +
          prediction.predictRelated.mkString(Const.LIST_SEP) + "\t" +
          context + "\t" +
          word_features + "\t" +
          holing_features + "\t" +
          target_holing_features + "\t" +
          "%.3f".format(prediction.predictConf) + "\t" +
          "%.3f".format(prediction.usedFeaturesNum) + "\t" +
          "%.3f".format(prediction.bestConfNorm) + "\t" +
          prediction.sensePriors.mkString(Const.LIST_SEP) + "\t" +
          prediction.usedFeatures.mkString(Const.LIST_SEP) + "\t" +
          prediction.allFeatures.mkString(Const.LIST_SEP)
      }
      .saveAsTextFile(outputPath)
  }

  def main(args: Array[String]) {
    if (args.size < 4) {
      println("Usage: clusters coocs deps trigrams lexsample output [wsd-mode] [use-prior-prob] [max-num-features] [partitions-num]")
      return
    }

    val conf = new SparkConf().setAppName("WSDEvaluation")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val clustersPath = args(0)
    val coocsPath = args(1)
    val depsPath = args(2)
    val trigramsPath = args(3)
    val lexSamplePath = args(4)
    val outputPath = args(5)
    val wsdMode = if (args.length > 6) WSDFeatures.fromString(args(6)) else WSD_MODE
    val usePriorProb = if (args.length > 7) args(7).toLowerCase().equals("true") else USE_PRIOR_PROB
    val maxNumFeatures = if (args.length > 8) args(8).toInt else MAX_FEATURE_NUM
    val partitionsNum = if (args.length > 9) args(9).toInt else PARTITIONS_NUM

    println("Cluster features: " + clustersPath)
    println("Co-occurrence features: " + coocsPath)
    println("Dependency features: " + depsPath)
    println("Trigram features: " + trigramsPath)
    println("Lexical sample dataset: " + lexSamplePath)
    println("Output: " + outputPath)
    println("WSD mode: " + wsdMode)
    println("Use prior probs.: " + usePriorProb)
    println("Max feature num: " + maxNumFeatures)
    println("Number of partitions of sense features: " + partitionsNum)

    if (!Util.exists(coocsPath) && !Util.exists(depsPath)) {
      println("Error: either coocs or dependency features must exist.")
      return
    } else if (!Util.exists(coocsPath)) {
      println("Warning: coocs features not available. Using only deps features.")
    } else if (!Util.exists(depsPath)) {
      println("Warning: deps features not available. Using only coocs features.")
    }
    Util.delete(outputPath)

    run(sc, lexSamplePath, outputPath, clustersPath, coocsPath, depsPath, trigramsPath, usePriorProb, wsdMode,
      maxNumFeatures, partitionsNum)
  }
}

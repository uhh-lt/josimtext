package de.uhh.lt.jst.wsd

import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.utils.{Const, KryoDiskSerializer, Util}
import de.uhh.lt.jst.wsd.WSDFeatures.WSDFeatures
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import scopt.Read
import scopt.Read.reads

import scala.util.Random

/**
  * Created by sasha on 13/06/17.
  */
object WSD extends SparkJob {

  val TARGET_FEATURES_BOOST = 3
  // boost strong but sparse target features
  val SYMMETRIZE_DEPS = true
  val DEBUG = false
  val FEATURES_POSTFIX = "-features"
  val SAVE_FEATURES = true

  case class Config(
    clusterDir: String = "",
    coocsDir: String = "",
    depsDir: String = "",
    trigramsDir: String = "",
    lexSamplePath: String = "",
    outputDir: String = "",
    usePriorProb: Boolean = true,
    wsdMode: WSDFeatures = WSDFeatures.DepstargetCoocsClustersTrigramstarget,
    maxNumFeatures: Int = 1000000,
    partitionsNum: Int = 16
  )

  implicit val readWSDFeatures: Read[WSDFeatures] = reads { WSDFeatures.fromString }

  override type ConfigType = Config

  override val config = Config()
  override val description = ""
  override val parser = new Parser {

    val validateDirExists = (dir: String) => {
      if (Util.exists(dir))
        success
      else
        failure(s"'$dir' does not exist.")
    }

    arg[String]("CLUSTER_DIR").action( (x, c) =>
      c.copy(clusterDir = x) ).required().hidden()

    arg[String]("COOCS_DIR").action( (x, c) =>
      c.copy(coocsDir = x) ).required().hidden()

    arg[String]("DEPS_DIR").action( (x, c) =>
      c.copy(depsDir = x) ).required().hidden()

    arg[String]("TRIGRAMS_DIR").action( (x, c) =>
      c.copy(trigramsDir = x) ).required().hidden()

    arg[String]("LEXSAMPLE_DIR").action( (x, c) =>
      c.copy(trigramsDir = x) ).required().hidden()

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(lexSamplePath = x) ).required().hidden()

    opt[Unit]('n', "noPrior").action( (x, c) =>
      c.copy(usePriorProb = false) ).text("")

    opt[WSDFeatures]('m', "wsdMode").action( (x, c) =>
      c.copy(wsdMode = x) ).text("")

    opt[Int]('f', "maxNumFeatures").action( (x, c) =>
      c.copy(maxNumFeatures = x) ).text("")

    opt[Int]('p', "paritionsNum").action( (x, c) =>
      c.copy(partitionsNum = x) ).text("")

    checkConfig( c =>
      if (!Util.exists(c.coocsDir) && !Util.exists(c.depsDir)) {
        failure("Either coocs or dependency features must exist.")
      } else {
        if (!Util.exists(c.coocsDir))
          println("Warning: coocs features not available. Using only deps features.")
        if (!Util.exists(c.depsDir))
          println("Warning: deps features not available. Using only coocs features.")
        success
      }
    )
  }

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
  def parseFeatures(word: String, featuresStr: String, maxFeaturesNum: Int = config.maxNumFeatures): Map[String, Double] = {

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

  type IndexedTermScores = Map[Int, Map[String, Double]]
  type FiveColumns[T] = (T, T, T, T, T)

  def buildFeatures(spark: SparkSession, config: Config): RDD[(String, FiveColumns[IndexedTermScores])] = {
    val sc = spark.sparkContext
    val featuresPath = config.clusterDir + FEATURES_POSTFIX
    val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    val featuresExist = fs.exists(new org.apache.hadoop.fs.Path(featuresPath))
    println(s"$featuresPath: $featuresExist")

    if (featuresExist) {
      val loadedFeatures = KryoDiskSerializer.objectFile[(String, FiveColumns[IndexedTermScores])](sc, featuresPath)
      loadedFeatures
    } else {
      // Load clusters senses and the associated features (required)
      // senses and features are the format: (lemma, (sense -> (feature -> prob)))
      val clustersCols = loadFile2Cols(sc, config.clusterDir).persist()

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
        if (WSDFeatures.clustersNeeded(config.wsdMode)) {
          loadCols2Features(clustersCols, config.maxNumFeatures, config.partitionsNum)
        } else {
          noFeatures
        }
      println(s"#words (cluster features): ${clusterFeatures.count()}")

      val coocFeatures =
        if (WSDFeatures.coocsNeeded(config.wsdMode) && Util.exists(config.coocsDir)) {
          val cols = loadFile2Cols(sc, config.coocsDir)
          loadCols2Features(cols, config.maxNumFeatures, config.partitionsNum)
        } else {
          if (WSDFeatures.coocsNeeded(config.wsdMode) && !Util.exists(config.coocsDir)) println(s"error: not found ${config.coocsDir}")
          noFeatures
        }
      println(s"#words (coocs features): ${clusterFeatures.count()}")

      val depsFeatures =
        if (WSDFeatures.depsNeeded(config.wsdMode) && Util.exists(config.depsDir)) {
          val cols = loadFile2Cols(sc, config.depsDir)
          loadCols2Features(cols, config.maxNumFeatures, config.partitionsNum)
        } else {
          if (WSDFeatures.depsNeeded(config.wsdMode) && !Util.exists(config.depsDir)) println(s"error: not found ${config.depsDir}")
          noFeatures
        }
      println(s"#words (deps features): ${depsFeatures.count()}")

      val trigramFeatures =
        if (WSDFeatures.trigramsNeeded(config.wsdMode) && Util.exists(config.trigramsDir)) {
          val cols = loadFile2Cols(sc, config.trigramsDir)
          loadCols2Features(cols, config.maxNumFeatures, config.partitionsNum)
        } else {
          if (WSDFeatures.trigramsNeeded(config.wsdMode) && !Util.exists(config.trigramsDir)) println(s"error: not found ${config.trigramsDir}")
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

  type TwelveColumns[T] = (T, T, T, T, T, T, T, T, T, T, T, T)

  def run(spark: SparkSession, config: Config): Unit = {

    val sc = spark.sparkContext
    // Load lexical sample
    // target, (dataset, features)
    // dataset: context_id	target	target_pos	target_position	gold_sense_ids	predict_sense_ids	golden_related	predict_related	context word_features	holing_features	target_holing_features
    val lexSample: RDD[(String, (Array[String], TwelveColumns[String]))] = sc.textFile(config.lexSamplePath)
      .map(line => line.split("\t", -1))
      .map { case Array(context_id, target, target_pos, target_position, gold_sense_ids, predict_sense_ids, golden_related, predict_related, context, word_features, holing_features, target_holing_features) =>
        (target, (
          getContextFeatures(word_features, target_holing_features, holing_features,  config.wsdMode),
          (context_id, target, target_pos, target_position, gold_sense_ids, predict_sense_ids, golden_related, predict_related, context, word_features, holing_features, target_holing_features)))
      case _ => ("", (Array[String](), ("", "", "", "", "", "", "", "", "", "", "", "")))
      }
      .cache()
    println(s"# lexical samples: ${lexSample.count()}")

    val features = buildFeatures(spark, config)

    val result = lexSample
      .join(features)
      .map { case (target, ((contextT, dataset), (inventoryT, clustersT, coocsT, depsT, trigramsT))) =>
        (dataset, predict(contextT, inventoryT, clustersT, coocsT, depsT, trigramsT, config.usePriorProb))
      }

    println(s"#classified lex samples: ${result.count()}")
    Util.delete(config.outputDir)

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
      .saveAsTextFile(config.outputDir)
  }
}

package de.tudarmstadt.lt.jst.wsd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import de.tudarmstadt.lt.jst.utils.{Const, Util}

object SensesFilter {
  def main(args: Array[String]) {
    if (args.size < 3) {
      println("Usage: SensesFilter <input-senses-fpath> <vocabulary-fpath> <output-senses-fpath> <lowercase>")
      println("<input-senses-fpath>\tpath to a csv file with sense clusters in the 'JS' format 'target<TAB>sense<TAB>cluster', cluster being 'word:sim'<SPACE><SPACE>'")
      println("<vocabulary>\tpath to a list of target words that will be kept in the output sense clusters")
      println("<output-senses-fpath>\tpath to the output senses in the same format as input, but containing only vocabulary target words")
      println("<lowercase>\tIgnore case in vocabulary and sense inventory: true or false.")
      println("<only-lower-or-first-upper>\tIf yes only 'apple' or 'Apple' are kept, but not 'APPLE' or 'AppLe': true or false.")

      return
    }

    val inSensesPath = args(0)
    val inVocPath = args(1)
    val outSensesPath = args(2)
    val lowercase = args(3).toBoolean
    val lowerOrFirstUpper = args(4).toBoolean


    val conf = new SparkConf().setAppName("JST: SensesFilter")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    runCli(sc, inSensesPath, inVocPath, outSensesPath, lowercase, lowerOrFirstUpper)
  }

  def runCli(sc: SparkContext, inSensesPath: String, inVocPath: String, outSensesPath: String,
             lowercase: Boolean, lowerOrFirstUpper:Boolean) = {
    val outVocPath = inVocPath + "-voc.csv"
    println("Input senses: " + inSensesPath)
    println("Input vocabulary:" + inVocPath)
    println("Output senses: " + outSensesPath)
    println("Output vocabulary: " + outVocPath)
    println("Lowercase: " + lowercase)

    Util.delete(outSensesPath)
    Util.delete(outVocPath)

    val voc = Util.loadVocabulary(sc, inVocPath)
    val (senses, clusterVoc) = run(inSensesPath, voc, sc, lowercase)

    senses
      .map({ case (target, sense_id, cluster) => target + "\t" + sense_id + "\t" + cluster })
      .saveAsTextFile(outSensesPath)

    clusterVoc
      .saveAsTextFile(outVocPath)
  }

  def run(inSensesPath: String, voc: Set[String], sc: SparkContext, lowercase: Boolean = true, lowerOrFirstUpper:Boolean = true): (RDD[(String, String, String)], RDD[String]) = {
    val senses: RDD[(String, String, String)] = sc.textFile(inSensesPath)
      .map { line => line.split("\t") }
      .map {
        case Array(target, sense_id, cluster) => (target, sense_id, cluster)
        case Array(target, sense_id, cluster, isas) => (target, sense_id, cluster)
        case _ => ("?", "0", "") }
      .filter { case (target, sense_id, cluster) => voc.contains(if (lowercase) target.toLowerCase() else target) }
      .filter {case (target, sense_id, cluster) => !lowerOrFirstUpper || target.length <= 1 || target.substring(1).toLowerCase() == target.substring(1)}
    val clusterVoc = senses
      .map { case (target, sense_id, cluster) => (cluster) }
      .flatMap { case (cluster) => cluster.split(Const.LIST_SEP) }
      .map { case cluster => cluster.split(":")(0) }
      .distinct()
      .sortBy { case cluster => cluster }

    (senses, clusterVoc)
  }
}
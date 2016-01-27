import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SensesFilter {
  def main(args: Array[String]) {
    if (args.size < 3) {
      println("Usage: SensesFilter <input-senses-fpath> <vocabulary-fpath> <output-senses-fpath>")
      println("<input-senses-fpath>\tpath to a csv file with sense clusters in the 'JS' format 'target<TAB>sense<TAB>keyword<TAB>cluster', cluster being 'word:sim'<SPACE><SPACE>'")
      println("<mwe-vocabulary-csv>\tpath to a list of target words that will be kept in the output sense clusters")
      println("<output-senses-fpath>\tpath to the output senses in the same format as input, but containing only vocabulary target words")
      return
    }

    // Input parameters
    val inSensesPath = args(0)
    val inVocPath = args(1)
    val outSensesPath = args(2)
    val outVocPath = args(2) + "-voc.csv"

    println("Input senses: " + inSensesPath)
    println("Input vocabulary:" + inVocPath)
    println("Output senses: " + outSensesPath)
    println("Output vocabulary: " + outVocPath)

    Util.delete(outSensesPath)
    Util.delete(outVocPath)


    // Set Spark configuration
    val conf = new SparkConf().setAppName("FreqFilter")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.dynamicAllocation.enabled", "true")
    //conf.set("spark.shuffle.service.enabled", "true") // required to enable dynamicAllocation
    val sc = new SparkContext(conf)

    // Filter
    val voc = Util.loadVocabulary(sc, inVocPath)
    val (senses, clusterVoc) = run(inSensesPath, voc, sc)

    // Save results
    senses
      .map({case (target, sense_id, keyword, cluster) => target + "\t" + sense_id + "\t" + keyword + "\t" + cluster})
      .saveAsTextFile(outSensesPath)

    clusterVoc
      .saveAsTextFile(outVocPath)
  }

  def run(inSensesPath: String, voc: Set[String], sc: SparkContext): (RDD[(String, String, String, String)], RDD[String]) = {

    val senses: RDD[(String, String, String, String)] = sc.textFile(inSensesPath)
      .map(line => line.split("\t"))
      .map({ case Array(target, sense_id, keyword, cluster) => (target, sense_id, keyword, cluster) case _ => ("?", "0", "?", "") })
      .filter({ case (target, sense_id, keyword, cluster) => (voc.contains(target.toLowerCase())) })
      .cache()

    val clusterVoc = senses
      .map({ case (target, sense_id, keyword, cluster) => (cluster) })
      .flatMap({ case (cluster) => cluster.split("  ") })
      .map({ case cluster => cluster.split(":")(0) })
      .distinct()
      .sortBy({ case cluster => cluster })
      .cache()
    (senses, clusterVoc)
  }
}
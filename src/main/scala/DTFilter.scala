import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import Util.delete
import org.apache.spark.rdd.RDD

object DTFilter {
  def main(args: Array[String]) {
    if (args.size < 2) {
      println("Usage: DTFilter <dt-path.csv> <mwe-vocabulary.csv> <output-dt-directory>")
      println("<dt.csv> is a distributional thesaurus in the format 'word_i<TAB>word_j<TAB>similarity_ij<TAB>features_ij'")
      println("<mwe-vocabulary.csv> is a list of words that the program will keep (word_i and word_j must be in the list)")
      return
    }

    // Input parameters
    val dtPath = args(0)
    val vocPath = args(1)
    val outPath = args(2)
    println("Input DT: " + dtPath)
    println("Vocabulary:" + vocPath)
    println("Output DT: " + outPath)

    // Set Spark configuation
    val conf = new SparkConf().setAppName("DTFilter")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // Action
    Util.delete(outPath)

    //val words:Set[String] = Set("Валя", "Валантеном", "Вадан")
    val voc = sc.textFile(vocPath)
      .map(line => line.split("\t"))
      .map({case Array(word) => (word)})
      .collect()
      .toSet

    val dt = sc.textFile(dtPath)
      .map(line => line.split("\t"))
      .map({case Array(word_i, word_j, sim_ij, features_ij) => (word_i, word_j, sim_ij, features_ij)})
      .filter({case (word_i, word_j, sim_ij, features_ij) => (!word_i.contains(" ") || voc.contains(word_i) && (!word_j.contains(" ") || voc.contains(word_j)))})
      .map({case (word_i, word_j, sim_ij, features_ij) => word_i + "\t" + word_j + "\t" + sim_ij})
      .saveAsTextFile(outPath)
  }
}

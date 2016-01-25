import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object DTFilter {
  def main(args: Array[String]) {
    if (args.size < 4) {
      println("Usage: DTFilter <dt-path.csv> <mwe-vocabulary.csv> <output-dt-directory> <keep-single-words>")
      println("<dt.csv>\tis a distributional thesaurus in the format 'word_i<TAB>word_j<TAB>similarity_ij<TAB>features_ij'")
      println("<mwe-vocabulary.csv>\tis a list of words that the program will keep (word_i and word_j must be in the list)")
      println("<output-dt-directory>\toutput directory with the filtered distributional thesaurus")
      println("<keep-single-words>\tif 'true' then all single words are kept even if they are not in the <vocabulary.csv>. default -- 'true'.")
      return
    }

    // Input parameters
    val dtPath = args(0)
    val vocPath = args(1)
    val outPath = args(2)
    val keepSingleWords = args(3).toBoolean
    println("Input DT: " + dtPath)
    println("Vocabulary:" + vocPath)
    println("Output DT: " + outPath)
    println("Keep single words: " + keepSingleWords)

    // Set Spark configuration
    val conf = new SparkConf().setAppName("DTFilter")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // Action
    val voc = sc.textFile(vocPath)
      .map(line => line.split("\t"))
      .map({case Array(word) => (word.toLowerCase())})
      .collect()
      .toSet

    val dt = sc.textFile(dtPath)
      .map(line => line.split("\t"))
      .map({case Array(word_i, word_j, sim_ij, features_ij) => (word_i, word_j, sim_ij, features_ij) case _ => ("?", "?", "?", "?") })

    val dt_filter =
      if(keepSingleWords){
        dt.filter({case (word_i, word_j, sim_ij, features_ij) => (!word_i.contains(" ") || voc.contains(word_i.toLowerCase()) && (!word_j.contains(" ") || voc.contains(word_j.toLowerCase())))})
      } else {
        dt.filter({case (word_i, word_j, sim_ij, features_ij) => (voc.contains(word_i.toLowerCase()) && (voc.contains(word_j.toLowerCase())))})
      }

    dt_filter
      .map({case (word_i, word_j, sim_ij, features_ij) => word_i + "\t" + word_j + "\t" + sim_ij})
      .saveAsTextFile(outPath)
  }
}
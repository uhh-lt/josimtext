import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object FreqFilter {
  def main(args: Array[String]) {
    if (args.size < 4) {
      println("Usage: FreqFilter <freq-csv> <mwe-vocabulary-csv> <output-freq-csv> <keep-single-words>")
      println("<freq-csv>\tpath to a csv with n-gram word counts 'word<TAB>freq'")
      println("<mwe-vocabulary-csv>\tpath to a list of words that will be keept in the output (word must be in the list)")
      println("<output-freq-csv>\tpath to output with the filtered word counts")
      println("<keep-single-words>\tif 'true' then all single words are kept even if they are not in the <vocabulary.csv>. default -- 'true'.")
      return
    }

    // Input parameters
    val freqPath = args(0)
    val vocPath = args(1)
    val outPath = args(2)
    val keepSingleWords = args(3).toBoolean
    println("Input frequency dictionary: " + freqPath)
    println("Vocabulary:" + vocPath)
    println("Output frequency dictionary: " + outPath)
    println("Keep single words: " + keepSingleWords)
    Util.delete(outPath)

    // Set Spark configuration
    val conf = new SparkConf().setAppName("FreqFilter")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.shuffle.service.enabled", "true") // required to enable dynamicAllocation
    val sc = new SparkContext(conf)

    // Load target vocabulary in node's memory
    val voc = sc.textFile(vocPath)
      .map(line => line.split("\t"))
      .map({case Array(word) => (word.toLowerCase())})
      .collect()
      .toSet

    // Filter the freq
    val freq = sc.textFile(freqPath)
      .map(line => line.split("\t"))
      .map({case Array(word, freq) => (word, freq) case _ => ("?", "?") })

    val freqFiltered =
      if(keepSingleWords){
        freq.filter({case (word, freq) => (!word.contains(" ") || voc.contains(word.toLowerCase()) )})
      } else {
        freq.filter({case (word, freq) => (voc.contains(word.toLowerCase()) )})
      }

    // Save the result
    freqFiltered
      .map({case (word, freq) => word + "\t" + freq})
      .saveAsTextFile(outPath)
  }
}
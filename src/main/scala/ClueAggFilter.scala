import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object ClueAggFilter {

  val POSTFIX = "-voc"

  def main(args: Array[String]) {
    if (args.size < 5) {
      println("Usage: WordFeatureFilter <vocabulary-csv> <senses-csv> <words-csv> <word-features-csv> <features-csv>")
      return
    }

    // Input parameters
    val vocPath = args(0)
    val sensesPath = args(1)
    val wordsPath = args(2)
    val wordFeaturesPath = args(3)
    val featuresPath = args(4)
    val sensesOutPath = sensesPath + POSTFIX
    val wordsOutPath = wordsPath + POSTFIX
    val wordFeaturesOutPath = wordFeaturesPath + POSTFIX
    val featuresOutPath = featuresPath + POSTFIX

    println("Vocabulary: " + vocPath)
    println("Senses: " + sensesPath)
    println("Words:" + wordsPath)
    println("Word-Features: " + wordFeaturesPath)
    println("Features: " + featuresPath)
    println("Senses output: " + sensesOutPath)
    println("Words output:" + wordsOutPath)
    println("Word-Features output: " + wordFeaturesOutPath)
    println("Features output: " + featuresOutPath)
    Util.delete(sensesOutPath)
    Util.delete(wordsOutPath)
    Util.delete(wordFeaturesOutPath)
    Util.delete(featuresOutPath)

    // Set Spark configuration
    val conf = new SparkConf().setAppName("FreqFilter")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(conf)

    val voc = Util.loadVocabulary(sc, vocPath)
    val (senses, clusterVocRDD) = SensesFilter.run(sensesPath, voc, sc)
    senses
      .map({case (target, sense_id, keyword, cluster) => target + "\t" + sense_id + "\t" + keyword + "\t" + cluster})
      .saveAsTextFile(sensesOutPath)

    val clusterVoc = clusterVocRDD.collect().toSet.union(voc)
    val words = FreqFilter.run(wordsPath, clusterVoc, false, sc)
    words
      .map({case (word, freq) => word + "\t" + freq})
      .saveAsTextFile(wordsOutPath)

    val (wordFeatures, featureVocRDD) = WordFeatureFilter.run(wordFeaturesPath, clusterVoc, sc)
    val featureVoc = featureVocRDD.collect().toSet
    wordFeatures
      .map({case (word, feature, freq) => word + "\t" + feature + "\t" + freq})
      .saveAsTextFile(wordFeaturesOutPath)

    val features = FreqFilter.run(featuresPath, featureVoc, false, sc)
    features
      .map({case (feature, freq) => feature + "\t" + freq})
      .saveAsTextFile(featuresOutPath)
  }
}
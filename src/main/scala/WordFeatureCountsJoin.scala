import org.apache.spark.{SparkConf, SparkContext}

object WordFeatureCountsJoin {
    def main(args: Array[String]) {
        if (args.size < 1) {
            println("Usage: WordFeatureCountsJoin word-feature-counts word-counts feature-counts output [wordlist]")
            return
        }

        val wordFeatureCountsFile = args(0)
        val wordCountsFile = args(1)
        val featureCountsFile = args(2)
        val outDir = args(3)

        val words:Set[String] = if (args.length > 4) args(4).split(",").toSet else null

        val conf = new SparkConf().setAppName("WordSim")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        val wordFeatureCounts = sc.textFile(wordFeatureCountsFile)
            .map(line => line.split("\t"))
            .map({case Array(word, feature, count) => (word, (feature, count.toInt))})

        val wordCounts = sc.textFile(wordCountsFile)
            .map(line => line.split("\t"))
            .map({case Array(word, count) => (word, count.toInt)})
            .filter({case (word, count) => words == null || words.contains(word)})

        val featureCounts = sc.textFile(featureCountsFile)
            .map(line => line.split("\t"))
            .map({case Array(feature, count) => (feature, count.toInt)})

        WordSimUtil.computeFeatureScores(wordFeatureCounts, wordCounts, featureCounts,
            Int.MaxValue, 2, 2, 2, 0, 100, WordSimUtil.lmi, outDir)
    }
}

import org.apache.spark.{SparkConf, SparkContext}

object WordFeatureScoresFromCounts {
    def main(args: Array[String]) {
        if (args.size < 1) {
            println("Usage: WordSim word-feature-counts word-counts feature-counts output [w=1000] [s=0.0] [t=2] [sig=LMI] [p=1000] [l=200]")
            println("For example, the arguments \"wikipedia wikipedia-out 500 0.0 3\" will override w with 500 and t with 3, leaving the rest at the default values")
            return
        }

        val wordFeatureCountsFile = args(0)
        val wordCountsFile = args(1)
        val featureCountsFile = args(2)
        val outDir = args(3)
        val param_w = if (args.size > 4) args(4).toInt else 1000
        val param_s = if (args.size > 5) args(5).toDouble else 0.0
        val param_t_wf = if (args.size > 6) args(6).toInt else 2
        val param_t_w = if (args.size > 7) args(7).toInt else 2
        val param_t_f = if (args.size > 8) args(8).toInt else 2
        val param_sig = if (args.size > 9) args(9) else "LMI"
        val param_p = if (args.size > 10) args(10).toInt else 1000

        val words:Set[String] = if (args.length > 11) args(11).split(",").toSet else null

        def sig(_n:Long, wc:Long, fc:Long, bc:Long) =
            if (param_sig == "LMI") WordSimUtil.lmi(_n,wc,fc,bc)
            else if (param_sig == "DESC") WordSimUtil.descriptivity(wc,fc,bc)
            else if (param_sig == "COV") WordSimUtil.cov(_n,wc,fc,bc)
            else if (param_sig == "FREQ") WordSimUtil.freq(_n,wc,fc,bc)
            else WordSimUtil.ll(_n,wc,fc,bc)

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
            param_w, param_t_wf, param_t_w, param_t_f, param_s, param_p, sig, outDir)
    }
}

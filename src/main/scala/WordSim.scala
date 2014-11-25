import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordSim {
    def main(args: Array[String]) {
        if (args.size < 1) {
            println("Usage: WordSim dataset [output] [w=1000] [s=0.0] [t=2] [sig=LMI] [p=1000] [l=200]")
            println("For example, the arguments \"wikipedia wikipedia-out 500 0.0 3\" will override w with 500 and t with 3, leaving the rest at the default values")
            return
        }

        val param_dataset = args(0)
        val outDir = if (args.size > 1) args(1) else param_dataset + "__"
        val param_w = if (args.size > 2) args(2).toInt else 1000
        val param_s = if (args.size > 3) args(3).toDouble else 0.0
        val param_t = if (args.size > 4) args(4).toInt else 2
        val param_sig = if (args.size > 5) args(5) else "LMI"
        val param_p = if (args.size > 6) args(6).toInt else 1000
        val param_l = if (args.size > 7) args(7).toInt else 200

        def sig(_n:Long, wc:Long, fc:Long, bc:Long) = if (param_sig == "LMI") WordSimUtil.lmi(_n,wc,fc,bc) else WordSimUtil.ll(_n,wc,fc,bc)

        val conf = new SparkConf().setAppName("WordSim")
        val sc = new SparkContext(conf)
        val file = sc.textFile(param_dataset)

        val (wordFeatureCounts, wordCounts, featureCounts) = WordSimUtil.computeWordFeatureCounts(file, outDir)
        val wordSimsWithFeatures = WordSimUtil.computeWordSimsWithFeatures(wordFeatureCounts, wordCounts, featureCounts,
                                                                 param_w, param_t, param_t, param_t, param_s, param_p, param_p, param_l, sig, outDir)

        wordSimsWithFeatures
            .map({case (word1, (word2, score, featureSet)) => word1 + "\t" + word2 + "\t" + score + "\t" + featureSet.mkString("  ")})
            .saveAsTextFile(outDir + "__SimWithFeatures")
    }
}

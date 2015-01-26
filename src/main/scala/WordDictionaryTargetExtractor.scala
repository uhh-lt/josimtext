import org.apache.spark.{SparkConf, SparkContext}

object WordDictionaryTargetExtractor {
    def main(args: Array[String]) {
        if (args.size < 1) {
            println("Usage: WordDictionaryTargetExtraxtor dictionary-path")
            return
        }

        val param_dataset = args(0)
        val outDir = if (args.size > 1) args(1) else param_dataset + "__Targets"

        val conf = new SparkConf().setAppName("WordSim")
        val sc = new SparkContext(conf)
        val file = sc.textFile(param_dataset)

        file.map(line => line.split("\t"))
            .flatMap({case Array(word, wc, numTargets, targets) => for (target <- targets.split("  ")) yield word + "\t" + WSDEvaluation.splitLastN(target, ":", 2)(0)})
            .saveAsTextFile(outDir)
        }
}

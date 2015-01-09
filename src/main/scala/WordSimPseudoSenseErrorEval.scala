import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object WordSimPseudoSenseErrorEval {

    def computeRelativeError(word1:String, word2:String, scores:Array[Float]):(Float, Float) = {
        if (scores.length == 2) {
            (
                math.abs(scores(0) - scores(1)) / math.max(scores(0), scores(1)),
                math.abs(scores(0) - scores(1))
            )
        } else (1,1) // if the other sense does not appear at all that's equivalent to sim = 0
    }

    def main(args: Array[String]) {
        if (args.size < 1) {
            println("Usage: WordSimPseudoSenseErrorEval sims [output]")
            return
        }

        val param_dataset = args(0)
        val outDir = if (args.size > 1) args(1) else param_dataset + "__"

        val conf = new SparkConf().setAppName("WordSim")
        val sc = new SparkContext(conf)

        val res = sc.textFile(param_dataset)
            .map(line => line.split("\t"))
            .filter(array => array.length >= 3) // filter erroneous lines
            .map({case Array(word1, word2, score, _*) => ((word1, word2.replaceAll("\\$\\$.*", "")), score.toFloat)})
            .groupByKey()
            .map({case ((word1, word2), scores) => (word1.replaceAll("\\$\\$.*", ""), (computeRelativeError(word1, word2, scores.toArray), 1))})
            .reduceByKey({case (((score11, score12), aggr1), ((score21, score22), aggr2)) => ((score11+score21,score12+score22), aggr1+aggr2)})
            .map({case (word, ((relErrorSum, absErrorSum), aggr)) => (word, (relErrorSum / aggr, absErrorSum / aggr))})

        res.map({case (word, (relErrorSum, absErrorSum)) => word + "\t" + relErrorSum + "\t" + absErrorSum})
           .saveAsTextFile(outDir + "AvgRelErrorPerWord")

        res.map({case (word, (avgRelError, avgAbsError)) => ("TOTAL", ((avgRelError, avgAbsError), 1))})
            .reduceByKey({case (((score11, score12), aggr1), ((score21, score22), aggr2)) => ((score11+score21,score12+score22), aggr1+aggr2)})
            .map({case (word, ((avgRelErrorSum, avgAbsErrorSum), aggr)) => (word, (avgRelErrorSum / aggr, avgAbsErrorSum / aggr))})
            .map({case (word, (relErrorSum, absErrorSum)) => word + "\t" + relErrorSum + "\t" + absErrorSum})
            .saveAsTextFile(outDir + "AvgRelErrorTotal")


    }
}

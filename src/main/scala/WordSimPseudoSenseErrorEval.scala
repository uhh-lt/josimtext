import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object WordSimPseudoSenseErrorEval {

    def computeRelativeError(word1:String, word2:String, scores:Array[Float]):(Float, Float, Float) = {
        if (scores.length == 2) {
            (
                math.abs(scores(0) - scores(1)) / math.max(scores(0), scores(1)),
                math.abs(scores(0) - scores(1)),
                (scores(0) + scores(1)) / 2
            )
        } else (1,scores(0),scores(0) / 2) // if the other sense does not appear at all that's equivalent to sim = 0
    }

    def computeDiscriminativeScore(word:String, list:List[Float]):Float = {
        if (list.size == 1) return 0
        val sortedList = list.sortWith({case (s1, s2) => s1 > s2})
        var relDiffSum = 0.0f
        for (i <- 1 to sortedList.size - 1) relDiffSum += (sortedList(i - 1) - sortedList(i)) / sortedList(i - 1)
        relDiffSum / (sortedList.size - 1)
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
            .map({case (word, ((score1, score2, sim), aggr)) => (word, ((score1, score2, sim), List(sim), aggr))})
            .reduceByKey({case (((score11, score12, sum1), simList1, aggr1), ((score21, score22, sum2), simList2, aggr2)) => ((score11+score21,score12+score22, sum1+sum2), simList1 ::: simList2, aggr1+aggr2)})
            .map({case (word, ((relErrorSum, absErrorSum, simSum), simList, aggr)) => (word, (relErrorSum / aggr, absErrorSum / aggr, simSum / aggr, computeDiscriminativeScore(word, simList), aggr))})

        res.map({case (word, (avgRelError, avgAbsError, simAvg, discrScore, simWordNum)) => word + "\t" + avgRelError + "\t" + avgAbsError + "\t" + simAvg + "\t" + discrScore + "\t" + simWordNum})
           .saveAsTextFile(outDir + "AvgRelErrorPerWord")

        res.map({case (word, (avgRelError, avgAbsError, simAvg, discrScore, simWordNum)) => ("TOTAL", ((avgRelError, avgAbsError, simAvg, discrScore, simWordNum), 1))})
            .reduceByKey({case (((score11, score12, simAvg1, discrScore1, simWordNum1), aggr1), ((score21, score22, simAvg2, discrScore2, simWordNum2), aggr2)) => ((score11+score21,score12+score22, simAvg1+simAvg2, discrScore1+discrScore2, simWordNum1+simWordNum2), aggr1+aggr2)})
            .map({case (word, ((avgRelErrorSum, avgAbsErrorSum, simAvgSum, discrScoreSum, simWordNumSum), aggr)) => (word, (avgRelErrorSum / aggr, avgAbsErrorSum / aggr, simAvgSum / aggr, discrScoreSum / aggr, simWordNumSum / aggr, aggr))})
            .map({case (word, (relErrorSum, absErrorSum, simAvgAvg, discrScoreAvg, simWordNumAvg, aggr)) => word + "\t" + relErrorSum + "\t" + absErrorSum + "\t" + simAvgAvg + "\t" + discrScoreAvg + "\t" + simWordNumAvg + "\t" + aggr})
            .saveAsTextFile(outDir + "AvgRelErrorTotal")


    }
}

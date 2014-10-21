import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordAndFeatureCount {
  def ll(n:Int, wc:Int, fc:Int, bc:Int): Double = {
    val wcL = math.log(wc) / math.log(2)
    val fcL = math.log(fc) / math.log(2)
    val bcL = math.log(bc) / math.log(2)
    val res = 2*(n*math.log(n) / math.log(2)
              -wc*wcL
              -fc*fcL
              +bc*bcL
              +(n-wc-fc+bc)*math.log(n-wc-fc+bc+0.000001) / math.log(2)
              +(wc-bc)*math.log(wc-bc+0.000001) / math.log(2)
              +(fc-bc)*math.log(fc-bc+0.000001) / math.log(2)
              -(n-wc)*math.log(n-wc+0.000001) / math.log(2)
              -(n-fc)*math.log(n-fc+0.000001) / math.log(2) )
    if ((n*bc)<(wc*fc)) -res else res
  }

  def lmi(n:Int, wc:Int, fc:Int, bc:Int): Double = {
    bc*math.log( (n*bc).toFloat/(wc*fc) )/math.log(2)
  }

  def main(args: Array[String]) {
    val param_t = 2;
    val param_w = 1000;
    val param_p = 1000;
    val param_s = 0.0;
    val param_l = 200;

    val dir = args(0)
    val conf = new SparkConf().setAppName("WordAndFeatureCount")
    val sc = new SparkContext(conf)
    val file = sc.textFile(dir)
    val wordFeatureCounts = file.map(line => line.split("	"))
                                .map(cols => (cols(0), (cols(1), cols(2).toInt)))
                                .filter({case (word, (feature, wfc)) => (wfc >= param_t)})
    
    wordFeatureCounts.cache
    val n = wordFeatureCounts.aggregate(0)(_ + _._2._2, _ + _)
    val wordCounts = wordFeatureCounts.map({case (word, (feature, wfc)) => (word, wfc)})
                                      .reduceByKey((wc1, wc2) => wc1 + wc2)
    wordCounts.cache
    var wordsPerFeature = wordFeatureCounts.map({case (word, (feature, wfc)) => (feature, word)})
                                           .groupByKey()
                                           .mapValues(v => v.toSet.size)
    //println("BEFORE " + wordsPerFeature.count)
    wordsPerFeature = wordsPerFeature.filter({case (feature, numWords) => numWords > 0 && numWords <= param_w})
    //println("AFTER " + wordsPerFeature.count)
    val featureCounts = wordFeatureCounts.map({case (word, (feature, wfc)) => (feature, wfc)})
                                         .reduceByKey((fc1, fc2) => fc1 + fc2)
                                         .join(wordsPerFeature)
    featureCounts.cache

    val res = wordFeatureCounts//.map(cols => (cols._2._1, (cols._1, cols._2._2)))
             .join(wordCounts)
             .map({case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc))})
             .join(featureCounts)
             .map({case (feature, ((word, wfc, wc), (fc, fwc))) => (word, (feature, ll(n, wc, fc, wfc)))})
             .groupByKey()
             // (word, [(feature, score), (feature, score), ...])
             .mapValues(featureScores => featureScores.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2})
                                                      .take(param_p)
                                                      .map(featureScore => featureScore._1)) // sort by value desc
    res.cache
    val res2 = res.cartesian(res)
                   .map({case ((word1, features1), (word2, features2)) => (word1, (word2, features1.toSet.intersect(features2.toSet).size))})
                   .filter({case (word1, (word2, sim)) => sim > 0})
    //res2.cache
    val res3 =    res2
                  .groupByKey()
                  .mapValues(simWords => simWords.toArray.sortWith({case ((_, s1), (_, s2)) => s1 > s2}).take(param_l))
                  .flatMap({case (word, simWords) => for(simWord <- simWords) yield (word, simWord)})
//    res2.cache()
//    val res3 = res2
//                  .sortBy(row => row._1.hashCode + 0.000001 * row._2._2.size)
//                  .mapValues(cols => (cols._1, cols._2.size, cols._2))
    
//    wordCounts.map(cols => cols._1 + "	" + cols._2)
//              .saveAsTextFile(dir + "__WordCount")
//    featureCounts.map(cols => cols._1 + "	" + cols._2)
//                 .saveAsTextFile(dir + "__FeatureCount")
//    print(res2)
    res3.map({case (word1, (word2, sim)) => word1 + "\t" + word2 + "\t" + sim}).saveAsTextFile(dir + "__LL_sims")
    /*val l = new Array[Int](600)
    for (i <- 0 to 599) {
      l(i) = i
    }
    val lRDD = sc.parallelize(l)
    print(lRDD.cartesian(lRDD).count())*/
//    print(wordCounts.cartesian(wordCounts).count())
  }
}

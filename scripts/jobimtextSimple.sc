// first, we parse some text corpus
val tokenize = (line: String) => line.toLowerCase.split("\\s+").toSeq
val text = sc.textFile("/Users/alex/Desktop/spark-notebook-0.6.3-scala-2.10.5-spark-1.6.0-hadoop-2.4.0/notebooks/corpus.txt")
val tokenized = text.map(tokenize)

// Let's define some types
type Jo = String
type Bim = String
type JoBim = (Jo, Bim)
type JoBimExtractor = Iterable[String] => Seq[JoBim]
type SignificanceMeasure = (Long, Long, Long, Long) => Double /* #words, #features, #word-feature, n */

// use the preceeding and succeeding token as context
val trigramContext: JoBimExtractor = tokens => tokens.sliding(3).toSeq.collect {
    case Seq(w1, w2, w3) => (w2, w1 + "_@_" + w3)
}

val jobims: RDD[JoBim] = tokenized.flatMap(trigramContext)

// count Jobims
val jobimCount: RDD[(JoBim, Int)] = jobims.map((_, 1)).reduceByKey(_ + _)
jobimCount.cache

// count Jos and Bims
val jocount: RDD[(Jo, Int)] = jobimCount.map{ case ((jo, bim), c) => (jo, c) }.reduceByKey(_ + _)
val bimcount: RDD[(Bim, Int)] = jobimCount.map{ case ((jo, bim), c) => (bim, c) }.reduceByKey(_ + _)

// repartition jobimCount by jo, join with jocount
val byJo: RDD[(Jo, (Bim, Int))] = jobimCount.map { case ((w, f), wfc) => (w, (f, wfc)) }
val withJocount: RDD[(Jo, ((Bim, Int), Int))] = byJo.join(jocount)

// repartition by bim, join with bimcount
val byBim: RDD[(Bim, (Jo, Int, Int))] = withJocount.map { case (w, ((f, wfc), wc)) => (f, (w, wc, wfc)) }
val withBimcount: RDD[(Bim, ((Jo, Int, Int), Int))] = byBim.join(bimcount)

// compute total number n
val n = jobimCount.values.reduce(_ + _)

// join featurecounts
val aggregated: RDD[(JoBim, (Long, Long, Long, Long))] = withBimcount
    .map { case (f, ((w, wc, wfc), fc)) => ((w, f), (wc, fc, wfc, n)) }


// first, lets define LMI
def lmi(n: Long, n_A: Long, n_B: Long, n_AB: Long): Double = {
    def log2(n:Double): Double =  math.log(n) / math.log(2)
    n_AB * (log2(n * n_AB) - log2(n_A * n_B))
}

// apply a significance measure
val sig: RDD[(Jo, (Bim, Double))]  = aggregated.mapValues((lmi: SignificanceMeasure).tupled).map { case ((jo, bim), w) => (jo, (bim, w)) }
sig.cache

// for output, construct 3 columns
sig.map { case (jo, (bim, w)) => (jo, bim, w) }.take(10)

/* Some pruning parameters */
val w = 100 // words per feature
val p = 100 // feature per word

val bimsPerJo = sig
    .groupByKey
    .mapValues { bims => bims.toList.sortBy(- _._2).take(p).map(_._1) }
    .cache

val josPerBim = bimsPerJo
    .flatMap({case (jo, bims) => for (b <- bims) yield (b, Set(jo))})
    .reduceByKey(_ union _)
    .collect { case (bim, jos) if jos.size <= w => (bim, jos.toList) }

val joSimilarity: RDD[(Jo, (Jo, Double))] = josPerBim
    .flatMap({case (bim, jos) => for(jo1 <- jos; jo2 <- jos) yield ((jo1, jo2), 1d)})
    .reduceByKey(_ + _)
    .map({case ((jo1, jo2), scoreSum) => (jo1, (jo2, scoreSum / p))})
    .sortBy({case (word, (simWord, score)) => (word, score)}, ascending=false)

// output
joSimilarity.map { case (jo1, (jo2, w)) => (jo1, jo2, w) }.take(1000)

import org.apache.hadoop.io.compress.GzipCodec

//val inputPath = "corpora/en/c4c/2015"
val inputPath = "corpora/en/c4c/2015-text/part-00000.gz"
val outputPath = "corpora/en/c4c/2015-text-wc-2/"

println(s"Input: $inputPath")
println(s"Output: $outputPath")

val corpus = sc.textFile(inputPath)

val seps = Array[Char](' ', '.')

val counts = corpus.flatMap(line => line.split(seps)).map(word => (word, 1)).reduceByKey(_ + _).map(wc => wc.swap).sortByKey(ascending=false).map({ case (freq, word) => s"$word\t$freq"}).saveAsTextFile(outputPath, classOf[GzipCodec])


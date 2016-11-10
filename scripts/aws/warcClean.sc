import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io.compress.GzipCodec

//val inputPath = "tmp/c4c/1m.txt"
//val inputPath = "tmp/c4c/1g.warc.gz"
val inputPath = "corpora/en/c4c/2015"
//val outputPath = "tmp/c4c/1g-output-2"
val outputPath = "corpora/en/c4c/2015-text"

val conf = new SparkConf().setAppName("WARC Filter")
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.hadoop.validateOutputSpecs", "false")
conf.set("spark.cleaner.ttl", "10000")
val sc = new SparkContext(conf)

val warcHeaderPattern = "^(WARC|c4_|Content-)".r
val warcCorpus = sc.textFile(inputPath)
val textCorpus = warcCorpus.filter(line => warcHeaderPattern.findFirstIn(line).isEmpty)
println(textCorpus.count())
textCorpus.saveAsTextFile(outputPath, classOf[GzipCodec])


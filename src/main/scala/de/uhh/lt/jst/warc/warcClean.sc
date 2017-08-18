import org.apache.spark.{SparkConf, SparkContext}

val inputPath = "s3://ukp-research-data/c4corpus/CC-MAIN-2016-07/cc-phase5out-2016-07/*_en_*" // ""s3://ukp-research-data/c4corpus/CC-MAIN-2016-07/cc-phase5out-2016-07/Lic_by-nc_Lang_en_*" // "s3://jobimtext/corpora/cc16/en/Lic_by-nc*"
val outputPath = "s3://jobimtext/corpora/commoncrawl-2016-en-html/"

val conf = new SparkConf().setAppName("WARC Filter")
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
val sc = new SparkContext(conf)

val warcHeaderPattern = "^(WARC|c4_|Content-)".r
val warcCorpus = sc.textFile(inputPath)
val textCorpus = warcCorpus.filter(line => warcHeaderPattern.findFirstIn(line).isEmpty)
textCorpus.saveAsTextFile(outputPath)


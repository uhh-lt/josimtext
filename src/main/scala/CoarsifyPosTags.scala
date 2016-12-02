
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source

object CoarsifyPosTags {
    val posLookup = Source
        .fromURL(getClass.getResource("/pos-tags.csv"))
        //.fromFile(getClass.getResource("/pos-tags.csv").getPath)
        .getLines
        .map{ _.split("\t") }
        .map{ case Array(freq, posOrig, posNew) => (posOrig, posNew) }
        .toMap

    def main(args: Array[String]) {
        if (args.size < 2) {
            println("Usage: CoarsifyPosTags <input-dir> <output-dir>")
            println("<input-dir>\tDirectory with word-feature counts (i.e. W-* files, F-* files, WF-* files).'")
            println("<output-dir>\tDirectory with output word-feature counts where W-* and WF-* files" +
                " contain coarsified POS tags (44 Penn POS tags --> 21 tags).")
            return
        }

        val inputPath = args(0)
        val outputPath = args(1)
        println("Input path: " + inputPath)
        println("Output frequency dictionary: " + outputPath)
        Util.delete(outputPath) // convinience for local tests

        val conf = new SparkConf().setAppName("CoarsifyPosTags")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        run(sc, inputPath, outputPath)
    }

    /**
      * Takeas as input a pos-tagged word like "Python#NNP" and outputs
      * another entry like "Python#NN". Each tag of MWE, e.g. "Python#NNP language#NN"
      * is transformed accordingly to "Python#NNP language#NN" */
    def coarsifyPosTag(lexitemWithPosTag: String) = {
        lexitemWithPosTag
            .split(" ")
            .map{ case wordWithPosTag => coarsifyWordPosTag(wordWithPosTag) }
            .mkString(" ")
    }

    def coarsifyWordPosTag(wordWithPosTag: String) = {
        val fields = wordWithPosTag.split(Const.POS_SEP)
        if (fields.length < 2) {
            wordWithPosTag
        } else {
            val head = fields
                .slice(0,fields.length-1)
                .mkString(Const.POS_SEP)

            val tail = posLookup.getOrElse(fields.last, fields.last)

            head + Const.POS_SEP + tail
        }
    }

    def run(sc: SparkContext, inputDir: String, outputDir: String) = {
        val outputWordCountsPath = outputDir + "/W"
        Util.delete(outputWordCountsPath)
        sc.textFile(inputDir + "/W-*")
            .map{ line => line.split("\t") }
            .map{ case Array(word, freq) => (word, freq.toLong) case _ => ("?", -1.toLong) }
            .map{ case (word, freq) => (coarsifyPosTag(word), freq) }
            .reduceByKey{ _ + _ }
            .map{ case (word,freq) => (freq, word)}
            .sortByKey(ascending = false)
            .map{ case (freq, word) => s"$word\t$freq" }
            .saveAsTextFile(outputWordCountsPath)

        val outputWordFeatureCountsPath = outputDir + "/WF"
        Util.delete(outputWordFeatureCountsPath)
        sc.textFile(inputDir + "/WF-*")
            .map{ line => line.split("\t") }
            .map{ case Array(word, feature, freq) => (word, feature, freq.toLong) case _ => ("?", "?", -1.toLong) }
            .map{ case (word, feature, freq) => ( (coarsifyPosTag(word), feature), freq) }
            .reduceByKey{ _ + _ }
            .map{ case ((word, feature), freq) => s"$word\t$feature\t$freq" }
            .saveAsTextFile(outputWordFeatureCountsPath)

        val outputFeaturesCountsPath = outputDir + "/F"
        Util.delete(outputFeaturesCountsPath)
        sc.textFile(inputDir + "/F-*")
            .saveAsTextFile(outputFeaturesCountsPath) // just copy to new location
    }
}

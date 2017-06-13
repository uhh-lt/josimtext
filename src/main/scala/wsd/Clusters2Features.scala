package wsd

import org.apache.spark.{SparkConf, SparkContext}
import util.Util


object Clusters2Features {
    def main(args: Array[String]) {

        if (args.length < 2) {
            println("Makes a WSD features file 'word<TAB>sense-id<TAB>cluster<TAB>features' from the old format sense" +
                "inventory file 'word<TAB>sense-id<TAB>keyword<TAB>cluster', where 'features' are the same as the cluster.")
            println("Usage: <input-sense-inventory> <output-features>")

            return
        }
        val sensesPath = args(0)
        val outputPath = args(1)

        println("Senses: " + sensesPath)
        println("Output: " + outputPath)
        Util.delete(outputPath)

        val conf = new SparkConf().setAppName("JST: Clusters2Features")
        val sc = new SparkContext(conf)

        run(sensesPath, outputPath, sc)
    }

    def run(sensesPath: String, outputPath: String, sc: SparkContext): Unit = {

        Util.delete(outputPath)

        val clusters = sc
            .textFile(sensesPath)
            .map(line => line.split("\t"))
            .map { case Array(target, sense_id, keyword, cluster) => (target, sense_id, cluster) case _ => ("?", "-1", "") }
            .filter { case (target, sense_id, cluster) => target != "?" }
            .map { case (target, sense_id, cluster) => f"$target\t$sense_id\t$cluster\t$cluster" }
            .saveAsTextFile(outputPath)
    }
}

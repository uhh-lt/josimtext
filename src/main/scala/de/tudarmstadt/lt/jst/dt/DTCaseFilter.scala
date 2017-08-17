package de.tudarmstadt.lt.jst.dt

import org.apache.spark.{SparkConf, SparkContext}
import de.tudarmstadt.lt.jst.utils

object DTCaseFilter {
    def main(args: Array[String]) {
        if (args.size < 2) {
            println("Filters out target words that at not all small caps or first capital + all small caps")
            println("Usage: DTCaseFilter <de.tudarmstadt.lt.jst.dt-path.csv> <output-de.tudarmstadt.lt.jst.dt-directory>")
            println("<de.tudarmstadt.lt.jst.dt>\tis a distributional thesaurus in the format 'word_i<TAB>word_j<TAB>similarity_ij[<TAB>features_ij]'")
            println("<output-de.tudarmstadt.lt.jst.dt-directory>\toutput directory with the filtered distributional thesaurus")
            return
        }

        val dtPath = args(0)
        val outPath = args(1)

        val conf = new SparkConf().setAppName("DTFilter")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        run(sc, dtPath, outPath)
    }

    def run(sc:SparkContext, dtPath:String, outPath:String) = {
        println("Input DT: " + dtPath)
        println("Output DT: " + outPath)
        utils.Util.delete(outPath)

        val dt = sc.textFile(dtPath)
            .map(line => line.split("\t"))
            .map{
                case Array(word_i, word_j, sim_ij, features_ij) => (word_i, word_j, sim_ij, features_ij)
                case Array(word_i, word_j, sim_ij) => (word_i, word_j, sim_ij, "?")
                case _ => ("?", "?", "?", "?")
            }

        val dt_filter = dt
            .filter{ case (word_i, word_j, sim_ij, features_ij) => (word_i.length <= 1) || (word_i.substring(1).toLowerCase() == word_i.substring(1)) }
            .map{ case (word_i, word_j, sim_ij, features_ij) => word_i + "\t" + word_j + "\t" + sim_ij }
            .saveAsTextFile(outPath)
    }
}
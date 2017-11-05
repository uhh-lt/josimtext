package de.uhh.lt.jst.dt

import de.uhh.lt.jst.{Job, utils}
import org.apache.spark.{SparkConf, SparkContext}

object DTFilter extends Job {

  case class Config(
    dt: String = "",
    vocabulary: String = "",
    outputDTDirectory: String = "",
    keepSingleWords: String = "true",
    filterOnlyTarget: String = "false"
  )

  type ConfigType = Config
  override val config = Config()

  override val command: String = "DTFilter"
  override val description = "Remove all target and related words which are not in the VOC_FILE."

  val parser = new Parser {

    opt[Unit]('s', "remove-single").action( (x, c) =>
      c.copy(keepSingleWords = "false") ).
      text("remove all rows with single words")

    opt[Unit]('t', "only-target").action( (x, c) =>
      c.copy(filterOnlyTarget = "true") ).
      text("only remove target words not related words")

    arg[String]("DT_FILE").action( (x, c) =>
      c.copy(dt = x) ).required().hidden()

    arg[String]("VOC_FILE").action( (x, c) =>
      c.copy(vocabulary = x) ).required().hidden()

    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDTDirectory = x) ).required().hidden()
  }

  override def run(config: Config): Unit = oldMain(config.productIterator.map(_.toString).toArray)

  // ------ unchanged old logic ------- //

  def oldMain(args: Array[String]) {
    if (args.size < 4) {
      println("Usage: DTFilter <dt-path.csv> <mwe-vocabulary.csv> <output-dt-directory> <keep-single-words>")
      println("<dt>\tis a distributional thesaurus in the format 'word_i<TAB>word_j<TAB>similarity_ij<TAB>features_ij'")
      println("<vocabulary>\tis a list of words that the program will keep (word_i and word_j must be in the list)")
      println("<output-dt-directory>\toutput directory with the filtered distributional thesaurus")
      println("<keep-single-words>\tif 'true' then all single words are kept even if they are not in the <vocabulary.csv>.")
      println("<filter-only-target>\tif 'true' then only target words will be filtered and all related words will be kept.")
      return
    }

    val dtPath = args(0)
    val vocPath = args(1)
    val outPath = args(2)
    val keepSingleWords = args(3).toBoolean
    val filterOnlyTarget = args(4).toBoolean

    val conf = new SparkConf().setAppName("DTFilter")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    run(sc, dtPath, vocPath, outPath, keepSingleWords, filterOnlyTarget)
  }

  def run(sc: SparkContext, dtPath: String, vocPath: String, outPath: String, keepSingleWords: Boolean, filterOnlyTarget: Boolean) = {
    println("Input DT: " + dtPath)
    println("Vocabulary:" + vocPath)
    println("Output DT: " + outPath)
    println("Keep all single words: " + keepSingleWords)
    println("Filter only targets: " + filterOnlyTarget)

    utils.Util.delete(outPath)

    val voc = sc.textFile(vocPath)
      .map(line => line.split("\t"))
      .map { case Array(word) => (word.trim().toLowerCase()) }
      .collect()
      .toSet
    println(s"Vocabulary size: ${voc.size}")

    val dt = sc.textFile(dtPath)
      .map(line => line.split("\t"))
      .map {
        case Array(word_i, word_j, sim_ij, features_ij) => (word_i, word_j, sim_ij, features_ij)
        case Array(word_i, word_j, sim_ij) => (word_i, word_j, sim_ij, "")
        case _ => ("?", "?", "?", "?")
      }

    val dt_filter =
      if (keepSingleWords && filterOnlyTarget) {
        dt.filter { case (word_i, word_j, sim_ij, features_ij) => voc.contains(word_i.toLowerCase()) || !word_i.contains(" ") }
      } else if (!keepSingleWords && filterOnlyTarget) {
        dt.filter { case (word_i, word_j, sim_ij, features_ij) => voc.contains(word_i.toLowerCase()) }
      } else if (keepSingleWords && !filterOnlyTarget) {
        dt.filter { case (word_i, word_j, sim_ij, features_ij) => (!word_i.contains(" ") || voc.contains(word_i.toLowerCase()) && (!word_j.contains(" ") || voc.contains(word_j.toLowerCase()))) }
      } else { // if (!keepSingleWords && !filterOnlyTarget){
        dt.filter { case (word_i, word_j, sim_ij, features_ij) => (voc.contains(word_i.toLowerCase()) && (voc.contains(word_j.toLowerCase()))) }
      }

    dt_filter
      .map { case (word_i, word_j, sim_ij, features_ij) => word_i + "\t" + word_j + "\t" + sim_ij }
      .saveAsTextFile(outPath)
  }

}
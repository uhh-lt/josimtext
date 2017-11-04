package de.uhh.lt.jst.dt

import de.uhh.lt.jst.Job
import org.apache.spark.{SparkConf, SparkContext}

// TODO delete
object WordSimFromDataSet extends Job{

  case class Config(
    dataset: String = "",
    output: String = "",
    w: String = "1000",
    s: String = "0.0",
    t: String = "2",
    sig: String = "LMI",
    p: String = "1000",
    l: String = "200"
  )

  override type ConfigType = Config
  override val config = Config()

  override val command: String = "WordSimFromDataSet"
  override val description = "[deprecated] Compute word similarity from a special data set (see: WordSimLib.computeWordFeatureCounts)"

  override val parser = new Parser {

    opt[String]("w").action( (x, c) =>
      c.copy(w = x) ).text("(default 1000)")

    opt[String]("s").action( (x, c) =>
      c.copy(s = x) ).text("(default 0.0)")

    opt[String]("t").action( (x, c) =>
      c.copy(t = x) ).text("(default 2)")

    opt[String]("sig").action( (x, c) =>
      c.copy(sig = x) ).text("(default LMI)")

    opt[String]("p").action( (x, c) =>
      c.copy(p = x) ).text("(default 1000)")

    opt[String]("l").action( (x, c) =>
      c.copy(l = x) ).text("(default 200)")

    arg[String]("dataset").action( (x, c) =>
      c.copy(dataset = x) ).required().text("")

    arg[String]("output").action( (x, c) =>
      c.copy(output = x) ).required().text("")

  }

  def run(config: Config): Unit =
    oldMain(config.productIterator.map(_.toString).toArray)

  // ------ unchanged old logic ------- //

  def oldMain(args: Array[String]) {
    if (args.size < 1) {
      // FIXME Bug: the usage text below shows output in brackets,
      // FIXME: suggesting it is optional, however `args.size < 1` above
      // FIXME: enforces two arguments, this makes output required if no other argument is given
      println("1Usage: WordSim dataset [output] [w=1000] [s=0.0] [t=2] [sig=LMI] [p=1000] [l=200]")
      println("For example, the arguments \"wikipedia wikipedia-out 500 0.0 3\" will override w with 500 and t with 3, leaving the rest at the default values")
      return
    }

    val param_dataset = args(0)
    val outDir = if (args.size > 1) args(1) else param_dataset + "__"
    val param_w = if (args.size > 2) args(2).toInt else 1000
    val param_s = if (args.size > 3) args(3).toDouble else 0.0
    val param_t = if (args.size > 4) args(4).toInt else 2
    val param_sig = if (args.size > 5) args(5) else "LMI"
    val param_p = if (args.size > 6) args(6).toInt else 1000
    val param_l = if (args.size > 7) args(7).toInt else 200
    val sig = "LMI"

    val conf = new SparkConf().setAppName("WordSim")
    val sc = new SparkContext(conf)
    val file = sc.textFile(param_dataset)

    val (wordFeatureCounts, wordCounts, featureCounts) = WordSimLib.computeWordFeatureCounts(file, outDir)
    val (wordSimsPath, wordSimsWithFeaturesPath, featuresPath) = WordSimLib.computeWordSimsWithFeatures(wordFeatureCounts, wordCounts, featureCounts, outDir, param_w, param_p, param_t, param_t, param_t, param_s, param_l, sig)
  }
}

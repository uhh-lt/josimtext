package de.uhh.lt.jst

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

abstract class SparkJob extends BaseJob {

  protected val additionalSparkConf = new SparkConf()

  private[this] def buildSpark = {
    val builder = SparkSession.builder()

    builder.config(additionalSparkConf)

    builder
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
  }


  private[this] def printSparkConf(spark: SparkSession): Unit = {
    val conf = spark.sparkContext.getConf
    println("\nSpark Configuration:")
    println("---------------")
    conf.getAll.foreach{ case (k,v) => println(s"$k=$v")}
    println("---------------\n")
  }

  override def run(config: ConfigType): Unit = {

    val spark = buildSpark

    if (debug) printSparkConf(spark)

    run(spark, config)
  }

  def run(spark: SparkSession, config: ConfigType): Unit

  abstract class Parser extends _BaseParser {

    opt[String]("spark-conf").
      valueName("<prop>=<value>").
      text(s"Arbitrary Spark configuration property.").
      validate( x =>
        if (x.contains("=")) success
        else failure(s"Invalid argument to --spark-conf: $x")
      ).
      action { (x, c) =>
        val (key, value) = x.split("=", 2) match {
          case Array(k,v) => (k,v) // x.split(_, 2) ensures array is not longer than 2
          case Array(k) => (k,"") // if no value is given, for example: spark.some.key=
        }
        additionalSparkConf.set(key, value)
        c
      }
  }

}
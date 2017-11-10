package de.uhh.lt.jst.dt.benchmarks

import de.uhh.lt.jst.dt.entities.DTEntry
import org.apache.spark.sql.{Dataset, SparkSession}
import de.uhh.lt.jst.dt._

object MeasureDTSim {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: MeasureDTSim <first_dt_path> <second_dt_path>")
      System.exit(1)
    }

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val firstPath = args(0)
    val secondPath = args(1)

    val measurements = compareDTs(spark, firstPath, secondPath)

    println(s"Similarity between distributional thesauri:")
    println(s"first: $firstPath")
    println(s"second: $secondPath")
    println("---------")
    println(s"$measurements")

  }

  def compareDTs(spark: SparkSession, path1: String, path2: String): Measurements = {
    val firstDT = readDT(spark, path1)
    val secondDT = readDT(spark, path2)

    compareDTs(firstDT, secondDT)
  }

  def compareDTs(dt1: Dataset[DTEntry], dt2: Dataset[DTEntry]): Measurements = {
    val totalInFirst = dt1.count()
    val totalInSecond = dt2.count()

    val commonInBoth = dt1.join(dt2, usingColumns = Seq("term1", "term2")).count

    Measurements(
      numCommon = commonInBoth,
      numMissing = totalInSecond - commonInBoth, // If only present in second, considered missing
      numExtra = totalInFirst - commonInBoth // If only present in first, considered extra
    )
  }

  def readDT(spark: SparkSession, path: String): Dataset[DTEntry] = {
    import spark.implicits._
    spark.read.dt(path).as[DTEntry]
  }

  case class Measurements(numCommon: Long, numMissing: Long, numExtra: Long) {

    def numEntriesInFirstDT: Long = numCommon + numMissing
    def numEntriesInSecondDT: Long = numCommon + numExtra

    // TODO: clarify whether this metric is good
    def simScore: Double = numCommon / (numCommon + numMissing + numExtra)

    override def toString: String = {
      val details = s"$numCommon common / --$numMissing / ++$numExtra"
      s"$simScore similarity score ($details)"
    }
  }
}

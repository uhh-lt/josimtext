package de.uhh.lt.jst.dt.benchmarks

import de.uhh.lt.jst.SparkJob
import de.uhh.lt.jst.dt.entities.DTEntry
import org.apache.spark.sql.{Dataset, SparkSession}
import de.uhh.lt.jst.dt._

object MeasureDTSim extends SparkJob {

  case class Config(firstDTPath: String = "", secondDTPath: String = "")

  override type ConfigType = Config
  override val config = Config()

  override val description: String = "Measures similarity of two distributional thesauri (DT)"

  override val parser = new Parser {
    arg[String]("FIRST_DT").action( (x, c) =>
      c.copy(firstDTPath = x) ).required().hidden()

    arg[String]("SECOND_DT").action( (x, c) =>
      c.copy(secondDTPath = x) ).required().hidden()
  }

  override def run(spark: SparkSession, config: Config): Unit = {

    val firstDT = readDT(spark, config.firstDTPath)
    val secondDT = readDT(spark, config.secondDTPath)
    compareDTs(firstDT, secondDT)
    val measurements = compareDTs(firstDT, secondDT)

    println(s"Similarity between distributional thesauri:")
    println(s"first: ${config.firstDTPath}")
    println(s"second: ${config.secondDTPath}")
    println("---------")
    println(s"$measurements")
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

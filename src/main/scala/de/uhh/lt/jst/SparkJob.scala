package de.uhh.lt.jst

import org.apache.spark.sql.SparkSession

abstract class SparkJob extends Job {
  override def run(config: ConfigType): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    run(spark, config)
  }

  def run(spark: SparkSession, config: ConfigType): Unit

}
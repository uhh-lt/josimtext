package de.tudarmstadt.lt

import org.apache.spark.{SparkConf, SparkContext}

trait SparkProvider {

  def appID: String = (this.getClass.getSimpleName
    + math.floor(math.random * 10E4).toLong.toString)

  def sparkConf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("josimtext").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.driver.host", "localhost")
  }

  def spark
}

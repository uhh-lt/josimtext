package de.uhh.lt.testing.spark

import com.holdenkarau.spark.testing.SparkContextProvider
import org.apache.spark.SparkConf

trait SparkConfigProvider

trait SparkESConfigProvider extends SparkConfigProvider {
  self: SparkContextProvider =>

  override def conf: SparkConf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.driver.host", "localhost").
      set("es.index.auto.create", "true").
      set("es.nodes", "localhost")
  }
}

package de.uhh.lt.jst

import org.apache.spark.{SparkConf, SparkContext}

abstract class SparkJob extends Job {

  override def run(config: ConfigType): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    run(sc, config)
  }

  def run(sc: SparkContext, config: ConfigType): Unit

}
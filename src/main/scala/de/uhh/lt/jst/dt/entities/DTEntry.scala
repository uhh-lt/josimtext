package de.uhh.lt.jst.dt.entities

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

case class DTEntry(term1: String, term2: String, score: Double) {

}

object DTEntry {
  val schema = StructType(Array(
    StructField("term1", StringType, nullable = false),
    StructField("term2", StringType, nullable = false),
    StructField("score", DoubleType, nullable = false)
  ))
}

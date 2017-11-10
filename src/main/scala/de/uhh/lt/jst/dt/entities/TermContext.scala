package de.uhh.lt.jst.dt.entities

import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class TermContext(term: String, context: String)

object TermContext {
  val schema = StructType(Array(
    StructField("term", StringType, nullable = false),
    StructField("context", StringType, nullable = false)
  ))
}

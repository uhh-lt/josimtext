package de.uhh.lt.jst

import de.uhh.lt.jst.dt.entities.DTEntry
import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object dt {

  // TODO: clarify whether one should refer to this CSV dialect as 'JoBimText CSV dialect' or something else?
  object JoBimTextCSVDialect {
    val options = Map(
      "delimiter" -> "\t",
      "quote" -> "",
      "header" -> "false"
    )
  }

  /**
    * Adds a method, `jbt`, to DataFrameReader that allows you to read a CSV file in JoBimText format
    * using the DataFileReader
    *
    * The JoBimText format is a CSV dialect:
    *
    *  delimiter: "\t",
    *  doubleQuote: false,
    *  quote char: \u0000,
    *  header: false
    */
  implicit class JBTDataFrameReader(reader: DataFrameReader) {
    def jbt: String => DataFrame = reader.options(JoBimTextCSVDialect.options).csv
  }


  /**
    * Adds a method, `dt`, to DataFrameReader that allows you to read a Distributional Thesaurus
    * file using the DataFileReader
    */
  implicit class DTDataFrameReader(reader: DataFrameReader) {
    def dt: String => DataFrame = (path: String) => reader
      .format("csv").options(JoBimTextCSVDialect.options).schema(DTEntry.schema).load(path)
  }
}

package de.uhh.lt.jst

import de.uhh.lt.jst.dt.entities.DTEntry
import org.apache.spark.sql.{DataFrame, DataFrameReader}

// TODO clarifiy
// - data model vs data format: https://www.coursera.org/learn/big-data-management/lecture/xZmuD/data-model-vs-data-format
// - Naming of files: freq vs count
// - Should we rename feature to context? Or at least put some note somewhere?

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
    */
  implicit class DTDataFrameReader(reader: DataFrameReader) {
    def dt: String => DataFrame = (path: String) => reader
      .format("csv").options(JoBimTextCSVDialect.options).schema(DTEntry.schema).load(path)
  }
}

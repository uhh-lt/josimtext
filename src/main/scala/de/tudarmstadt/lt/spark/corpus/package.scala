package de.tudarmstadt.lt.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader}


/**
  * For a good walk through of the Data Source API please take a look at:
  * https://michalsenkyr.github.io/2017/02/spark-sql_datasource
  *
  * This package was mostly copied over and then modified from:
  * https://github.com/databricks/spark-avro
  */
package object corpus {

  // TODO implement writer

  /**
    * Adds a method, `de.tudarmstadt.lt.jst.corpus`, to DataFrameReader that allows you to read de.tudarmstadt.lt.jst.corpus files using
    * the DataFileReader
    */
  implicit class CorpusDataFrameReader(reader: DataFrameReader) {
    def corpus: String => DataFrame = reader.format("de.tudarmstadt.lt.spark.corpus").load
  }
}
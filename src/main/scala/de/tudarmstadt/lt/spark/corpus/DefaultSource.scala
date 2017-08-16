package de.tudarmstadt.lt.spark.corpus

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.TextOutputWriter
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
  * Provides access to Corpus data from pure SQL statements.
  */
class DefaultSource extends TextBasedFileFormat with DataSourceRegister {

    override def shortName(): String = "de.tudarmstadt.lt.jbt.corpus"

    override def toString: String = "Corpus"

    private def verifySchema(schema: StructType): Unit = {
      if (schema.size != 1) {
        throw new Exception (
          s"Corpus data source supports only a single column, and you have ${schema.size} columns.")
      }
      val tpe = schema(0).dataType
      if (tpe != StringType) {
        throw new Exception(
          s"Corpus data source supports only a string column, but you have ${tpe.simpleString}.")
      }

    }

    override def inferSchema(
                              sparkSession: SparkSession,
                              options: Map[String, String],
                              files: Seq[FileStatus]): Option[StructType] = Some(new StructType().add("value", StringType))

    override def prepareWrite(
                               sparkSession: SparkSession,
                               job: Job,
                               options: Map[String, String],
                               dataSchema: StructType): OutputWriterFactory = {
      verifySchema(dataSchema)

      val conf = job.getConfiguration
      val compressionCodec = options.get("compression").map(CompressionCodecs.getCodecClassName)
      compressionCodec.foreach { codec =>
        CompressionCodecs.setCodecConfiguration(conf, codec)
      }

      new OutputWriterFactory {
        override def newInstance(
                                  path: String,
                                  dataSchema: StructType,
                                  context: TaskAttemptContext): OutputWriter = {
          new TextOutputWriter(path, dataSchema, context)
        }

        override def getFileExtension(context: TaskAttemptContext): String = {
          ".txt" + TextOutputWriter.getCompressionExtension(context)
        }
      }
    }

    override def buildReader(
                              sparkSession: SparkSession,
                              dataSchema: StructType,
                              partitionSchema: StructType,
                              requiredSchema: StructType,
                              filters: Seq[Filter],
                              options: Map[String, String],
                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
      assert(
        requiredSchema.length <= 1,
        "Corpus data source only produces a single data column named \"value\".")

      // hadoopConf.set("textinputformat.record.delimiter", options.getOrElse("delimiter", "\n\n"))
      val delimiter = options.getOrElse("delimiter", "\n\n")
      val broadcastedHadoopConf =
        sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

      (file: PartitionedFile) => {
        val reader = new HadoopFileReader(file, broadcastedHadoopConf.value.value, delimiter)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => reader.close()))

        if (requiredSchema.isEmpty) {
          val emptyUnsafeRow = new UnsafeRow(0)
          reader.map(_ => emptyUnsafeRow)
        } else {
          val unsafeRow = new UnsafeRow(1)
          val bufferHolder = new BufferHolder(unsafeRow)
          val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, 1)

          reader.map { line =>
            // Writes to an UnsafeRow directly
            bufferHolder.reset()
            unsafeRowWriter.write(0, line.getBytes, 0, line.getLength)
            unsafeRow.setTotalSize(bufferHolder.totalSize())
            unsafeRow
          }
        }
      }
    }
  }

/*
  class TextOutputWriter(
                          path: String,
                          dataSchema: StructType,
                          context: TaskAttemptContext)
    extends OutputWriter {

    private[this] val buffer = new Text()

    private val recordWriter: RecordWriter[NullWritable, Text] = {
      new TextOutputFormat[NullWritable, Text]() {
        override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
          new Path(path)
        }
      }.getRecordWriter(context)
    }

    override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

    override protected[sql] def writeInternal(row: InternalRow): Unit = {
      val utf8string = row.getUTF8String(0)
      buffer.set(utf8string.getBytes)
      recordWriter.write(NullWritable.get(), buffer)
    }

    override def close(): Unit = {
      recordWriter.close(context)
    }
  }
*/
/*
  object TextOutputWriter {
    /** Returns the compression codec extension to be used in a file name, e.g. ".gzip"). */
    def getCompressionExtension(context: TaskAttemptContext): String = {
      // Set the compression extension, similar to code in TextOutputFormat.getDefaultWorkFile
      if (FileOutputFormat.getCompressOutput(context)) {
        val codecClass = FileOutputFormat.getOutputCompressorClass(context, classOf[GzipCodec])
        ReflectionUtils.newInstance(codecClass, context.getConfiguration).getDefaultExtension
      } else {
        ""
      }
    }
  }


  private def readCorpus(
                       sparkSession: SparkSession,
                       inputPaths: Seq[String]): RDD[String] = {
    readText(sparkSession, options, inputPaths.mkString(","))
  }

  private def tokenRdd(
                        sparkSession: SparkSession,
                        options: CSVOptions,
                        header: Array[String],
                        inputPaths: Seq[String]): RDD[Array[String]] = {
    val rdd = baseRdd(sparkSession, options, inputPaths)
    // Make sure firstLine is materialized before sending to executors
    val firstLine = if (options.headerFlag) findFirstLine(options, rdd) else null
    CSVRelation.univocityTokenizer(rdd, firstLine, options)
  }

  private def readCorpus(
                        sparkSession: SparkSession,
                        options: CSVOptions,
                        location: String): RDD[String] = {
    if (Charset.forName(options.charset) == StandardCharsets.UTF_8) {
      sparkSession.sparkContext.textFile(location)
    } else {
      val charset = options.charset
      sparkSession.sparkContext
        .newAPIHadoopFile[LongWritable, Text, TextInputFormat](location)
        .mapPartitions(_.map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset)))
    }
  }

  
}
*/
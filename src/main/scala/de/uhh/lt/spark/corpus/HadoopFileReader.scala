/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.uhh.lt.spark.corpus

import java.io.Closeable
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, LineRecordReader}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.sql.execution.datasources.{PartitionedFile, RecordReaderIterator}

/**
 * Mostly a copy of
 * [[org.apache.spark.sql.execution.datasources.HadoopFileLinesReader]]
 * with the small change that a delimiter is provided to [[LineRecordReader]]
 */
class HadoopFileReader(
    file: PartitionedFile, conf: Configuration, delimiter: String) extends Iterator[Text] with Closeable {
  private val iterator = {
    val fileSplit = new FileSplit(
      new Path(new URI(file.filePath)),
      file.start,
      file.length,
      Array.empty)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    // Convert string to bytes array: https://stackoverflow.com/a/5250738
    // TODO is encoding needed? until now it worked!
    val charBytesDelimiter = delimiter.toCharArray.map(_.toByte)
    // Providing the delimiter to LineRecordReader does the trick:
    val reader = new LineRecordReader(charBytesDelimiter)
    reader.initialize(fileSplit, hadoopAttemptContext)
    new RecordReaderIterator(reader)
  }

  override def hasNext: Boolean = iterator.hasNext

  override def next(): Text = iterator.next()

  override def close(): Unit = iterator.close()
}

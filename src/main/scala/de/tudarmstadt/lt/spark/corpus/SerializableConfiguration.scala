
package de.tudarmstadt.lt.spark.corpus

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import org.apache.hadoop.conf.Configuration
import scala.util.control.NonFatal
import org.slf4j.LoggerFactory

/**
  * Copied from [[org.apache.spark.util.SerializableConfiguration]]
  */
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  @transient private[corpus] lazy val log = LoggerFactory.getLogger(getClass)
  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }

  /**
    * Copied from [[org.apache.spark.util.Utils]]
    */
  private def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        log.error("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        log.error("Exception encountered", e)
        throw new IOException(e)
    }
  }
}

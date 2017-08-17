package de.tudarmstadt.lt.jst.utils

import org.apache.spark.Partitioner

/**
 * Partitioner that uses the modulo of an (index) key as partition index
 */
class IndexModuloPartitioner(val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = (key.asInstanceOf[Long] % numPartitions).toInt
}

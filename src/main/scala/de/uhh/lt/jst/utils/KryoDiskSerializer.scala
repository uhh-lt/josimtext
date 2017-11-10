package de.uhh.lt.jst.utils

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Input
import de.uhh.lt.jst.SparkJob
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag


/**
  * Adapted from https://github.com/phatak-dev/blog/blob/master/code/kryoexample/src/main/scala/com/madhu/spark/kryo/KryoExample.scala
  **/

object KryoDiskSerializer extends SparkJob {
  case class Config(outputDir: String = "")
  override type ConfigType = Config
  override val config = Config()
  override val description = "Kryo example"
  override protected val parser = new Parser {
    arg[String]("OUTPUT_DIR").action( (x, c) =>
      c.copy(outputDir = x) ).required().hidden()
  }

  def run(spark: SparkSession, config: Config): Unit = {
    val sc = spark.sparkContext
    //create some dummy data
    val personList = 1 to 10000 map (value => new Person(value + ""))
    val personRDD = sc.makeRDD(personList)

    saveAsObjectFile(personRDD, config.outputDir)
    val rdd = objectFile[Person](sc, config.outputDir)
    println(rdd.map(person => person.name).collect().toList)
  }

  /*
   * Used to write as Object file using kryo serialization
   */
  def saveAsObjectFile[T: ClassTag](rdd: RDD[T], path: String) {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)

    rdd.mapPartitions(iter => iter.grouped(10)
      .map(_.toArray))
      .map(splitArray => {
        //initializes kyro and calls your registrator class
        val kryo = kryoSerializer.newKryo()

        //convert data to bytes
        val bao = new ByteArrayOutputStream()
        val output = kryoSerializer.newKryoOutput()
        output.setOutputStream(bao)
        kryo.writeClassAndObject(output, splitArray)
        output.close()

        // we are ignoring key field of sequence file
        val byteWritable = new BytesWritable(bao.toByteArray)
        (NullWritable.get(), byteWritable)
      }).saveAsSequenceFile(path)
  }

  /*
   * Method to read from object file which is saved kryo format.
   */
  def objectFile[T](sc: SparkContext, path: String, minPartitions: Int = 1)(implicit ct: ClassTag[T]) = {
    val kryoSerializer = new KryoSerializer(sc.getConf)

    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => {
        val kryo = kryoSerializer.newKryo()
        val input = new Input()
        input.setBuffer(x._2.getBytes)
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[Array[T]]
        dataObject
      })
  }

  // user defined class that need to serialized
  class Person(val name: String)

}

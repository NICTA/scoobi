package com.nicta.scoobi
package core

import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.compress.{GzipCodec, CompressionCodec}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import Data._

/**
 * An output store from a MapReduce job
 */
trait DataSink[K, V, B] extends Sink { outer =>
  lazy val id = ids.get

  def outputFormat: Class[_ <: OutputFormat[K, V]]
  def outputKeyClass: Class[K]
  def outputValueClass: Class[V]
  def outputConverter: OutputConverter[K, V, B]
  def outputCheck(implicit sc: ScoobiConfiguration)
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration)

  def outputCompression(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = new DataSink[K, V, B] {
    def outputFormat: Class[_ <: OutputFormat[K, V]]                = outer.outputFormat
    def outputKeyClass: Class[K]                                    = outer.outputKeyClass
    def outputValueClass: Class[V]                                  = outer.outputValueClass
    def outputConverter: OutputConverter[K, V, B]                   = outer.outputConverter
    def outputCheck(implicit sc: ScoobiConfiguration)               { outer.outputCheck(sc) }
    def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) { outer.outputConfigure(job) }

    /** configure the job so that the output is compressed */
    override def configureCompression(configuration: Configuration) = {
      configuration.set("mapred.output.compress", "true")
      configuration.set("mapred.output.compression.type", compressionType.toString)
      configuration.setClass("mapred.output.compression.codec", codec.getClass, classOf[CompressionCodec])
      this
    }
  }

  /** configure the compression for a given job */
  def configureCompression(configuration: Configuration) = this

  def isCompressed = {
    val configuration = new Configuration
    configureCompression(configuration)
    configuration.getBoolean("mapred.output.compress", false)
  }

  def outputPath(implicit sc: ScoobiConfiguration) = {
    val jobCopy = new Job(sc.conf)
    outputConfigure(jobCopy)
    Option(FileOutputFormat.getOutputPath(jobCopy))
  }

  private [scoobi]
  def write(values: Seq[_], recordWriter: RecordWriter[_,_]) {
    values foreach { value =>
      val (k, v) = outputConverter.toKeyValue(value.asInstanceOf[B])
      recordWriter.asInstanceOf[RecordWriter[K, V]].write(k, v)
    }
  }
}

/**
 * Internal untyped definition of a Sink to store result data
 */
private[scoobi]
trait Sink {
  /** unique id for this Sink */
  def id: Int
  /** The OutputFormat specifying the type of output for this DataSink. */
  def outputFormat: Class[_ <: OutputFormat[_, _]]
  /** The Class of the OutputFormat's key. */
  def outputKeyClass: Class[_]
  /** The Class of the OutputFormat's value. */
  def outputValueClass: Class[_]
  /** Maps the type consumed by this DataSink to the key-values of its OutputFormat. */
  def outputConverter: OutputConverter[_, _, _]
  /** Check the validity of the DataSink specification. */
  def outputCheck(implicit sc: ScoobiConfiguration)
  /** Configure the DataSink. */
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration)
  /** @return the path for this Sink. */
  def outputPath(implicit sc: ScoobiConfiguration): Option[Path]

  /** Set the compression configuration */
  def outputCompression(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK): Sink
  /** configure the compression for a given job */
  def configureCompression(configuration: Configuration): Sink

  def compress = compressWith(new GzipCodec)
  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = outputCompression(codec)

  /** @return true if this Sink is compressed */
  private[scoobi]
  def isCompressed: Boolean

  /** write values to this sink, using a specific record writer */
  private [scoobi]
  def write(values: Seq[_], recordWriter: RecordWriter[_,_])
}

object Data {
  object ids extends UniqueInt
}
/**
 * specify an object on which it is possible to add sinks and to compress them
 */
trait DataSinks {
  type T
  def addSink(sink: Sink): T
  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK): T
  def compress: T = compressWith(new GzipCodec)
}

/**
 * A Bridge is both a Source and a Sink.
 *
 * It has a bridgeStoreId which is a UUID string.
 *
 * The content of the Bridge can be read as an Iterable to retrieve materialised values
 */
private[scoobi]
trait Bridge extends Source with Sink {
  def bridgeStoreId: String
  def readAsIterable(implicit sc: ScoobiConfiguration): Iterable[_]
}




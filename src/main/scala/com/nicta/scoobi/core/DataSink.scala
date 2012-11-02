package com.nicta.scoobi
package core

import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.compress.{GzipCodec, CompressionCodec}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.fs.Path

/* An output store from a MapReduce job. */
trait DataSink[K, V, B] extends Sink { outer =>
  /** The OutputFormat specifying the type of output for this DataSink. */
  def outputFormat: Class[_ <: OutputFormat[K, V]]

  /** The Class of the OutputFormat's key. */
  def outputKeyClass: Class[K]

  /** The Class of the OutputFormat's value. */
  def outputValueClass: Class[V]

  /** Check the validity of the DataSink specification. */
  def outputCheck(implicit sc: ScoobiConfiguration)

  /** Configure the DataSink. */
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration)

  /** Maps the type consumed by this DataSink to the key-values of its OutputFormat. */
  def outputConverter: OutputConverter[K, V, B]

  /** Set the compression configuration */
  def outputCompression(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = new DataSink[K, V, B] {
    def outputFormat: Class[_ <: OutputFormat[K, V]]                = outer.outputFormat
    def outputKeyClass: Class[K]                                    = outer.outputKeyClass
    def outputValueClass: Class[V]                                  = outer.outputValueClass
    def outputCheck(implicit sc: ScoobiConfiguration)               = outer.outputCheck(sc)
    def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) = outer.outputConfigure(job)
    def outputConverter: OutputConverter[K, V, B]                   = outer.outputConverter
    /** configure the job so that the output is compressed */
    override def configureCompression(job: Job) = {
      job.getConfiguration.set("mapred.output.compress", "true")
      job.getConfiguration.set("mapred.output.compression.type", compressionType.toString())
      job.getConfiguration.setClass("mapred.output.compression.codec", codec.getClass, classOf[CompressionCodec])
      job
    }
  }

  /** configure the compression for a given job */
  def configureCompression(job: Job) = job

  /** @return the output path of this sink */
  def outputPath(implicit sc: ScoobiConfiguration) = {
    val jobCopy = new Job(sc.conf)
    outputConfigure(jobCopy)
    Option(FileOutputFormat.getOutputPath(jobCopy))
  }

  private [scoobi]
  def unsafeWrite(values: Seq[_], recordWriter: RecordWriter[_,_]) {
    values foreach { value =>
      val (k, v) = outputConverter.toKeyValue(value.asInstanceOf[B])
      recordWriter.asInstanceOf[RecordWriter[K, V]].write(k, v)
    }

  }

}

trait Sink {
  def outputCheck(implicit sc: ScoobiConfiguration)
  def outputFormat: Class[_ <: OutputFormat[_, _]]
  def outputKeyClass: Class[_]
  def outputValueClass: Class[_]
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration)
  def outputConverter: OutputConverter[_,_,_]
  def outputCompression(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK): Sink
  def configureCompression(job: Job): Job
  def outputPath(implicit sc: ScoobiConfiguration): Option[Path]

  def compress = compressWith(new GzipCodec)
  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = outputCompression(codec)

  private[scoobi] def isCompressed =
    configureCompression(new Job).getConfiguration.getBoolean("mapred.output.compress", false)

  private [scoobi]
  def unsafeWrite(values: Seq[_], recordWriter: RecordWriter[_,_])
}

trait Bridge extends Source with Sink {
  def readAsIterable(implicit sc: ScoobiConfiguration): Iterable[_]
}

/** Convert the type consumed by a DataSink into an OutputFormat's key-value types. */
trait OutputConverter[K, V, B] {
  def toKeyValue(x: B): (K, V)
}

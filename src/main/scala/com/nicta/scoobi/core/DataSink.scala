/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package core

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io.compress.{GzipCodec, CompressionCodec}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import Data._
import io.text.TextInput
import impl.ScoobiConfigurationImpl
import impl.rtt.ScoobiWritable

/**
 * An output store from a MapReduce job
 */
trait DataSink[K, V, B] extends Sink { outer =>
  lazy val id = ids.get
  lazy val stringId = id.toString

  def outputFormat(implicit sc: ScoobiConfiguration): Class[_ <: OutputFormat[K, V]]
  def outputKeyClass(implicit sc: ScoobiConfiguration): Class[K]
  def outputValueClass(implicit sc: ScoobiConfiguration): Class[V]
  def outputConverter: OutputConverter[K, V, B]
  def outputCheck(implicit sc: ScoobiConfiguration)
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration)

  def outputCompression(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = new DataSink[K, V, B] {
    def outputFormat(implicit sc: ScoobiConfiguration): Class[_ <: OutputFormat[K, V]] = outer.outputFormat
    def outputKeyClass(implicit sc: ScoobiConfiguration): Class[K]                     = outer.outputKeyClass
    def outputValueClass(implicit sc: ScoobiConfiguration): Class[V]                   = outer.outputValueClass
    def outputConverter: OutputConverter[K, V, B]                                      = outer.outputConverter
    def outputPath(implicit sc: ScoobiConfiguration): Option[Path]                     = outer.outputPath(sc)
    def outputCheck(implicit sc: ScoobiConfiguration)                                  { outer.outputCheck(sc) }
    def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration)                    { outer.outputConfigure(job) }
    override def outputSetup(implicit configuration: Configuration)                    { outer.outputSetup }

    /** configure the job so that the output is compressed */
    override def configureCompression(configuration: Configuration) = {
      configuration.set("mapred.output.compress", "true")
      configuration.set("mapred.output.compression.type", compressionType.toString)
      configuration.setClass("mapred.output.compression.codec", codec.getClass, classOf[CompressionCodec])
      this
    }

    override def toSource: Option[Source] = outer.toSource
  }

  /** configure the compression for a given job */
  def configureCompression(configuration: Configuration) = this

  def isCompressed = {
    val configuration = new Configuration
    configureCompression(configuration)
    configuration.getBoolean("mapred.output.compress", false)
  }

  def outputSetup(implicit configuration: Configuration) {}

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
trait Sink { outer =>
  /** unique id for this Sink */
  def id: Int
  /** unique id for this Sink, as a string. Can be used to create a file path */
  def stringId: String
  /** The OutputFormat specifying the type of output for this DataSink. */
  def outputFormat(implicit sc: ScoobiConfiguration): Class[_ <: OutputFormat[_, _]]
  /** The Class of the OutputFormat's key. */
  def outputKeyClass(implicit sc: ScoobiConfiguration): Class[_]
  /** The Class of the OutputFormat's value. */
  def outputValueClass(implicit sc: ScoobiConfiguration): Class[_]
  /** Maps the type consumed by this DataSink to the key-values of its OutputFormat. */
  def outputConverter: OutputConverter[_, _, _]
  /** Check the validity of the DataSink specification. */
  def outputCheck(implicit sc: ScoobiConfiguration)
  /** Configure the DataSink. */
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration)
  /** @return the path for this Sink. */
  def outputPath(implicit sc: ScoobiConfiguration): Option[Path]
  /** This method is called just before writing data to the sink */
  def outputSetup(implicit configuration: Configuration)

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

  /** implement this function if this sink can be turned into a Source */
  def toSource: Option[Source] = None
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
  def stringId = bridgeStoreId
  def readAsIterable(implicit sc: ScoobiConfiguration): Iterable[_]
}

object Bridge {
  def create(source: Source, sink: Sink, bridgeId: String): Bridge = new Bridge {
    def bridgeStoreId = bridgeId
    override def id = sink.id
    override def toString = "Bridge "+bridgeId+sink.outputPath(new ScoobiConfigurationImpl).map(path => " ("+path+")").getOrElse("")

    def inputFormat = source.inputFormat
    def inputCheck(implicit sc: ScoobiConfiguration) { source.inputCheck }
    def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) { source.inputConfigure(job) }
    def inputSize(implicit sc: ScoobiConfiguration) = source.inputSize
    def fromKeyValueConverter = source.fromKeyValueConverter
    private[scoobi] def read(reader: RecordReader[_,_], mapContext: InputOutputContext, read: Any => Unit) { source.read(reader, mapContext, read) }

    def outputFormat(implicit sc: ScoobiConfiguration) = sink.outputFormat
    def outputKeyClass(implicit sc: ScoobiConfiguration) = sink.outputKeyClass
    def outputValueClass(implicit sc: ScoobiConfiguration) = sink.outputValueClass
    def outputConverter = sink.outputConverter
    def outputCheck(implicit sc: ScoobiConfiguration) { sink.outputCheck }
    def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) { sink.outputConfigure(job) }
    def outputSetup(implicit configuration: Configuration) { sink.outputSetup }
    def outputPath(implicit sc: ScoobiConfiguration) = sink.outputPath
    def outputCompression(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = sink.outputCompression(codec, compressionType)
    def configureCompression(configuration: Configuration) = sink.configureCompression(configuration)
    private[scoobi] def isCompressed = sink.isCompressed
    private [scoobi] def write(values: Seq[_], recordWriter: RecordWriter[_,_]) { sink.write(values, recordWriter) }
    override def toSource: Option[Source] = Some(source)

    def readAsIterable(implicit sc: ScoobiConfiguration) =
      new Iterable[Any] { def iterator = Source.read(source, (a: Any) => a).iterator }
  }
}




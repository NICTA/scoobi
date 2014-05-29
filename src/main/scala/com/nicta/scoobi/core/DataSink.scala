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
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.{Configurable, Configuration}
import Data._
import com.nicta.scoobi.impl.io.{FileSystems, Files}
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.permission.FsAction
import FsAction._
import com.nicta.scoobi.impl.control.Exceptions._
import com.nicta.scoobi.impl.mapreducer.ChannelOutputFormat
import ChannelOutputFormat._

/**
 * An output store from a MapReduce job
 */
trait DataSink[K, V, B] extends Sink { outer =>
  private lazy val logger = LogFactory.getLog("scoobi.DataSink")

  val id = ids.get
  lazy val stringId = id.toString

  def outputFormat(implicit sc: ScoobiConfiguration): Class[_ <: OutputFormat[K, V]]
  def outputKeyClass(implicit sc: ScoobiConfiguration): Class[K]
  def outputValueClass(implicit sc: ScoobiConfiguration): Class[V]
  def outputConverter: OutputConverter[K, V, B]
  def outputCheck(implicit sc: ScoobiConfiguration)
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration)

  /** @return a compression object if this sink is compressed */
  def compression: Option[Compression]
  /** configure the compression for a given job */
  def configureCompression(configuration: Configuration) = {
    compression.filter { c =>
      val compressorAvailable = c.codec match {
        case configurable: CompressionCodec with Configurable =>
          configurable.setConf(configuration)
          trye(c.codec.createCompressor)(identity)

        case other =>
          trye(c.codec.createCompressor)(identity)
      }

      compressorAvailable.fold(
      { e => logger.debug(s"compressor not available for codec type ${c.codec.getClass}: $e"); false },
      { _ => logger.debug(s"compressor available for codec type ${c.codec.getClass}"); true })

    } map  { case Compression(codec, compressionType) =>
      configuration.set("mapred.output.compress", "true")
      configuration.set("mapred.output.compression.type", compressionType.toString)
      configuration.setClass("mapred.output.compression.codec", codec.getClass, classOf[CompressionCodec])
    } getOrElse {
      logger.debug(s"no compression set on this sink")
      configuration.set("mapred.output.compress", "false")
    }
    this
  }

  def isCompressed = compression.isDefined

  def outputSetup(implicit sc: ScoobiConfiguration) {}
  def outputTeardown(implicit sc: ScoobiConfiguration) {}

  private [scoobi]
  def write(values: Traversable[_], recordWriter: RecordWriter[_,_])(implicit configuration: Configuration) {
    values foreach { value =>
      val (k, v) = outputConverter.toKeyValue(value.asInstanceOf[B])
      recordWriter.asInstanceOf[RecordWriter[K, V]].write(k, v)
    }
  }
}

/** store the compression parameters for sinks */
case class Compression(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK)

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
  def outputSetup(implicit sc: ScoobiConfiguration)
  /** This method is called just after writing data to the sink */
  def outputTeardown(implicit sc: ScoobiConfiguration)

  /** configure the compression for a given job */
  def configureCompression(configuration: Configuration): Sink

  /** @return a new sink with Gzip compression enabled */
  def compress = compressWith(new GzipCodec)
  /** @return a new sink with compression enabled */
  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK): Sink

  /** @return true if this Sink is compressed */
  private[scoobi]
  def isCompressed: Boolean

  /** write values to this sink, using a specific record writer */
  private [scoobi]
  def write(values: Traversable[_], recordWriter: RecordWriter[_,_])(implicit configuration: Configuration)

  /** @return true if the file path has the name of an output channel with the proper tag and index or if it is a _SUCCESS file */
  def isSinkResult(tag: Int) =
    (f: Path) => f.getName match {
      case OutputChannelFileName(t, i) => t.toInt == tag && i.toInt == id
      case _                           => false
    }
}

object Sink {
  private lazy val logger = LogFactory.getLog("scoobi.Sink")

  /** default check for sink using output files */
  val defaultOutputCheck = (output: Path, overwrite: Boolean, sc: ScoobiConfiguration) => {
    if (Files.pathExists(output)(sc.configuration) && !overwrite) {
      throw new FileAlreadyExistsException("Output path already exists: " + output)
    } else logger.info("Output path: " + output.toUri.toASCIIString)

    Files.checkFilePermissions(output, WRITE)(sc.configuration)
  }

  val noOutputCheck = (output: Path, overwrite: Boolean, sc: ScoobiConfiguration) => ()

  type OutputCheck =  (Path, Boolean, ScoobiConfiguration) => Unit

}

/**
 * This is a Sink which can also be used as a Source
 */
trait SinkSource extends Sink {
  def toSource: Source

  /** @return the checkpoint parameters if this sink is a Checkpoint */
  def checkpoint: Option[Checkpoint]

  /** @return true if this sink is a checkpoint */
  def isCheckpoint: Boolean = checkpoint.isDefined

  /** @return true if this Sink is a checkpoint and has been filled with data */
  def checkpointExists(implicit sc: ScoobiConfiguration): Boolean =
    checkpoint.exists(_.isValid)

  /** @return the path of the checkpoint */
  def checkpointPath: Option[String] = checkpoint.map(_.pathAsString)

  override def outputSetup(implicit sc: ScoobiConfiguration) {
    checkpoint.map(_.setup)
  }

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
  def updateSinks(f: Seq[Sink] => Seq[Sink]): T
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
trait Bridge extends Source with Sink with SinkSource {
  def readAsIterable(implicit sc: ScoobiConfiguration): Iterable[_]
  def readAsValue(implicit sc: ScoobiConfiguration) = readAsIterable.iterator.next
}

object Bridge {
  def create(source: Source, sink: SinkSource, bridgeId: String): Bridge = new Bridge with SinkSource {
    def stringId = bridgeId
    override lazy val id = sink.id
    override def equals(a: Any) = a match {
      case o: Bridge => o.id == id
      case _         => false
    }
    override def hashCode = id.hashCode

    def checkpoint = sink.checkpoint
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
    override def outputSetup(implicit sc: ScoobiConfiguration) { super.outputSetup; sink.outputSetup }
    def outputTeardown(implicit sc: ScoobiConfiguration) { sink.outputTeardown }
    def outputPath(implicit sc: ScoobiConfiguration) = sink.outputPath
    def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = sink.compressWith(codec, compressionType)
    def configureCompression(configuration: Configuration) = sink.configureCompression(configuration)
    private[scoobi] def isCompressed = sink.isCompressed
    private [scoobi] def write(values: Traversable[_], recordWriter: RecordWriter[_,_])(implicit configuration: Configuration) { sink.write(values, recordWriter) }

    def toSource: Source = source

    def readAsIterable(implicit sc: ScoobiConfiguration) =
      new Iterable[Any] { def iterator = Source.read(source).iterator }
  }
}




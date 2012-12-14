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
package impl
package exec

import java.io._

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.serializer.Deserializer
import org.apache.hadoop.io.serializer.SerializationFactory
import org.apache.hadoop.io.serializer.Serializer
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.filecache.DistributedCache._
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException
import org.apache.commons.logging.LogFactory
import scala.collection.JavaConversions._

import Configurations._
import io.DataSource
import ChannelsInputFormat._
import util.JarBuilder
import scalaz.Scalaz._
import application.ScoobiConfiguration

/** An input format that delegates to multiple input formats, one for each
 * input channel. */
class ChannelsInputFormat[K, V] extends InputFormat[K, V] {
  private lazy val logger = LogFactory.getLog("scoobi."+getClass.getSimpleName)

  def getSplits(context: JobContext): java.util.List[InputSplit] = {

    getInputFormats(context).flatMap { case (channel, format) =>
      val conf = extractChannelConfiguration(context, channel)

      /**
       * Wrap each of the splits for this InputFormat, tagged with the channel number. InputSplits
       * will be queried in the Mapper to determine the channel being processed.
       *
       * We catch InvalidInputException in case of a missing input format file for a bridge store when the previous
       * MapReduce job didn't produce any file (@see issue #60)
       */
       try {
         format.getSplits(new Job(new Configuration(conf))).map { (pathSplit: InputSplit) =>
           new TaggedInputSplit(conf, channel, pathSplit, format.getClass.asInstanceOf[Class[_ <: InputFormat[_,_]]])
         }
       } catch {
         case e: InvalidInputException => {
           logger.debug("Could not get the splits for "+format.getClass.getName+". This is possibly because a previous MapReduce job didn't produce any output. See issue #60", e)
           Nil
         }
       }
    }.toList
  }

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[K, V] = {
    val taggedInputSplit = split.asInstanceOf[TaggedInputSplit]
    new ChannelRecordReader(
      taggedInputSplit,
      new TaskAttemptContextImpl(
        extractChannelConfiguration(context, taggedInputSplit.channel),
        context.getTaskAttemptID))
  }

}


/**
 * Object that allows for channels with different input format requirements
 * to be specified. Makes use of ChannelsInputFormat
 */
object ChannelsInputFormat {

  private val INPUT_FORMAT_PROPERTY = "scoobi.input.formats"

  /**
   * configure the sources for multiple channels:
   *
   * - add the necessary classes to the jar builder
   * - set the input format for each source
   * - configure the input channel for each source
   *
   */
  def configureSources(job: Job, jar: JarBuilder, sources: List[DataSource[_,_,_]])(implicit sc: ScoobiConfiguration) = {
    configureChannelsInputFormat(job) |>
    configureSourcesChannels(jar, sources)
  }

  private def configureSourcesChannels(jar: JarBuilder, sources: List[DataSource[_,_,_]])(implicit sc: ScoobiConfiguration) = (initial: Configuration) => {
    sources.zipWithIndex.foldLeft(initial) { case (conf, (source, channel)) =>
      conf |>
      configureSourceRuntimeClass(jar, source) |>
      configureInputChannel(channel, source)
    }
  }

  /** configure a new input channel on the job's configuration */
  private def configureInputChannel(channel: Int, source: DataSource[_,_,_])(implicit sc: ScoobiConfiguration) = (conf: Configuration) => {
    conf |>
    configureSourceInputFormat(source, channel) |>
    configureSource(source, channel)
  }

  private def configureChannelsInputFormat(job: Job): Configuration = {
    job.setInputFormatClass(classOf[ChannelsInputFormat[_,_]].asInstanceOf[Class[_ <: InputFormat[_,_]]])
    job.getConfiguration
  }

  private def configureSourceRuntimeClass(jar: JarBuilder, source: DataSource[_,_,_]) = (conf: Configuration) => {
    source match {
      case bs @ BridgeStore() => jar.addRuntimeClass(bs.rtClass.getOrElse(sys.error("Run-time class should be set.")))
      case _                  => ()
    }
    conf
  }
  /**
   * Record as configuration properties the channel number to InputFormat mappings
   * as described by the DataSource
   */
  private def configureSourceInputFormat(source: DataSource[_,_,_], channel: Int) = (conf: Configuration) => {
    conf.addValues(INPUT_FORMAT_PROPERTY, channel+";"+source.inputFormat.getName)
  }

  /**
   * Call the DataSource's configure method but with a proxy Job object. Then, add the
   * configuration properties added by the configure method to the main job's configuration
   * but prefixed with "scoobi.inputX", where X is the channel number.
   *
   * Also include the properties which might have been modified by the source (like the DistributedCache files)
   */
  private def configureSource(source: DataSource[_,_,_], channel: Int)(implicit sc: ScoobiConfiguration) = (conf: Configuration) => {
    val job = new Job(new Configuration(conf))
    source.inputConfigure(job)

    Option(job.getConfiguration.get(CACHE_FILES)).foreach { files =>
      conf.set(ChannelPrefix.prefix(channel, CACHE_FILES), files)
      conf.addValues(CACHE_FILES, files)
    }
    conf.updateWith(job.getConfiguration) { case (k, v)  if k != CACHE_FILES  => (ChannelPrefix.prefix(channel, k), v) }
  }

  /**
   * Configure a new configuration object for a given channel by extracting the channel keys from the current context
   *
   * @return a new Configuration from an existing context (for the configuration) and a channel id
   */
  private def extractChannelConfiguration(context: JobContext, channel: Int): Configuration = {
    val Prefix = ChannelPrefix.regex(channel)
    context.getConfiguration.updateWith { case (Prefix(k), v) if k != CACHE_FILES => (k, v) }
  }

  /** Get a map of all the input formats per channel id. */
  private def getInputFormats(context: JobContext): Map[Int, InputFormat[_,_]] = {
    val conf = context.getConfiguration
    val Entry = """(.*);(.*)""".r

    conf.get(INPUT_FORMAT_PROPERTY).split(",").toList.map {
      case Entry(ch, infmt) => (ch.toInt, ReflectionUtils.newInstance(Class.forName(infmt), conf).
                                                          asInstanceOf[InputFormat[_,_]])
    }.toMap
  }
}

object ChannelPrefix {
  private val INPUT_CHANNEL_PROPERTY = "scoobi.input"
  def prefix(channel: Int): String = INPUT_CHANNEL_PROPERTY + channel + ":"
  def prefix(channel: Int, k: String): String = prefix(channel) + k
  def regex(channel: Int) = (prefix(channel) + """(.*)""").r
}

/** A RecordReader that delegates the functionality to the underlying record reader
  * in TaggedInputSplit. */
class ChannelRecordReader[K, V](split: InputSplit, context: TaskAttemptContext) extends RecordReader[K, V] {

  private val originalRR: RecordReader[K, V] = {
    val taggedInputSplit: TaggedInputSplit = split.asInstanceOf[TaggedInputSplit]
    val inputFormat = taggedInputSplit.inputFormatClass.newInstance.asInstanceOf[InputFormat[K, V]]
    inputFormat.createRecordReader(taggedInputSplit.inputSplit, context)
  }

  override def close() { originalRR.close() }

  override def getCurrentKey: K = originalRR.getCurrentKey

  override def getCurrentValue: V = originalRR.getCurrentValue

  override def getProgress: Float = originalRR.getProgress

  override def initialize(split: InputSplit, context: TaskAttemptContext) =
    originalRR.initialize(split.asInstanceOf[TaggedInputSplit].inputSplit, context)

  override def nextKeyValue: Boolean = originalRR.nextKeyValue
}


/** A wrapper around an InputSplit that is tagged with an input channel id. Is
  * used with ChannelInputFormat. */
class TaggedInputSplit
    (private var conf: Configuration,
     var channel: Int,
     var inputSplit: InputSplit,
     var inputFormatClass: Class[_ <: InputFormat[_,_]])
  extends InputSplit with Configurable with Writable {

  def this() = this(null.asInstanceOf[Configuration], 0, null.asInstanceOf[InputSplit],
                    null.asInstanceOf[Class[_ <: InputFormat[_,_]]])

  def getLength: Long = inputSplit.getLength

  def getLocations: Array[String] = inputSplit.getLocations

  def readFields(in: DataInput) {
    channel = in.readInt
    val inputSplitClassName = Text.readString(in)
    inputSplit = ReflectionUtils.newInstance(Class.forName(inputSplitClassName), conf).asInstanceOf[InputSplit]
    val inputFormatClassName = Text.readString(in)
    inputFormatClass = Class.forName(inputFormatClassName).asInstanceOf[Class[_ <: InputFormat[_,_]]]

    val factory: SerializationFactory = new SerializationFactory(conf)
    val deserialiser: Deserializer[InputSplit] = factory.getDeserializer(inputSplit.getClass.asInstanceOf[Class[InputSplit]])
    deserialiser.open(in.asInstanceOf[DataInputStream])
    inputSplit = deserialiser.deserialize(inputSplit)
  }

  def write(out: DataOutput) {
    out.writeInt(channel)
    Text.writeString(out, inputSplit.getClass.getName)
    Text.writeString(out, inputFormatClass.getName)

    val factory: SerializationFactory = new SerializationFactory(conf)
    val serialiser: Serializer[InputSplit] = factory.getSerializer(inputSplit.getClass.asInstanceOf[Class[InputSplit]])
    serialiser.open(out.asInstanceOf[DataOutputStream])
    serialiser.serialize(inputSplit)
  }

  def getConf: Configuration = conf
  def setConf(conf: Configuration) { this.conf = conf }
}

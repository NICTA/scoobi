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
package mapreducer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException
import org.apache.commons.logging.LogFactory
import scala.collection.JavaConversions._
import scalaz.Scalaz._

import core._
import rtt.JarBuilder
import Configurations._
import ChannelsInputFormat._
import monitor.Loggable._
import impl.util.Compatibility
import Compatibility.hadoop2._
import java.io.IOException

/** An input format that delegates to multiple input formats, one for each
  * input channel. */
class ChannelsInputFormat[K, V] extends InputFormat[K, V] {
  private implicit lazy val logger = LogFactory.getLog("scoobi."+getClass.getSimpleName)

  def getSplits(context: JobContext): java.util.List[InputSplit] = {

    getInputFormats(context).flatMap { case (channel, (format, configuration)) =>
      /**
       * Wrap each of the splits for this InputFormat, tagged with the channel number. InputSplits
       * will be queried in the Mapper to determine the channel being processed.
       *
       * We catch InvalidInputException in case of a missing input format file for a bridge store when the previous
       * MapReduce job didn't produce any file (@see issue #60)
       */
      try {
        format.getSplits(Compatibility.newJob(configuration)).map { (pathSplit: InputSplit) =>
          new TaggedInputSplit(configuration, channel, pathSplit, format.getClass)
        }
      } catch {
        case e: InvalidInputException => {
          logger.debug("Could not get the splits for "+format.getClass.getName+". This is possibly because a previous MapReduce job didn't produce any output (see issue #60)", e)
          Nil
        }
        case e: IOException => {
          logger.debug("Could not get the splits for "+format.getClass.getName+". This is possibly because an input path has not been specified (see issue #283)", e)
          Nil
        }
      }
    }.toList
  }

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[K, V] = {
    val taggedInputSplit = split.asInstanceOf[TaggedInputSplit]
    new ChannelRecordReader(
      taggedInputSplit,
      Compatibility.newTaskAttemptContext(extractChannelConfiguration(context.getConfiguration, taggedInputSplit.channel),
                                          context.getTaskAttemptID))
  }

}


/**
 * Object that allows for channels with different input format requirements
 * to be specified. Makes use of ChannelsInputFormat
 */
object ChannelsInputFormat {
  private implicit lazy val logger = LogFactory.getLog("scoobi."+getClass.getSimpleName)

  private val INPUT_FORMAT_PROPERTY = "scoobi.input.formats"

  /**
   * configure the sources for multiple channels:
   *
   * - add the necessary classes to the jar builder
   * - set the input format for each source
   * - configure the input channel for each source
   *
   */
  def configureSources(job: Job, jar: JarBuilder, sources: Seq[Source])(implicit sc: ScoobiConfiguration) = {
    configureChannelsInputFormat(job) |>
      configureSourcesChannels(jar, sources)
  }

  private def configureSourcesChannels(jar: JarBuilder, sources: Seq[Source])(implicit sc: ScoobiConfiguration) = (initial: Configuration) => {
    sources.foldLeft(initial) { case (conf, source) =>
      conf |>
        configureSourceRuntimeClass(jar, source) |>
        configureInputChannel(source)
    }
  }

  /** configure a new input channel on the job's configuration */
  private def configureInputChannel(source: Source)(implicit sc: ScoobiConfiguration) = (conf: Configuration) => {
    conf |>
      configureSourceInputFormat(source) |>
      configureSource(source)
  }

  private def configureChannelsInputFormat(job: Job): Configuration = {
    job.setInputFormatClass(classOf[ChannelsInputFormat[_,_]].asInstanceOf[Class[_ <: InputFormat[_,_]]])
    job.getConfiguration
  }

  private def configureSourceRuntimeClass(jar: JarBuilder, source: Source) = (conf: Configuration) => {
    source match {
      case bs : BridgeStore[_] => {
        val rtClass = bs.rtClass(ScoobiConfiguration(conf)).debug(c => "adding the BridgeStore class "+c.clazz.getName+" from Source "+source.id+" to the configuration")
        jar.addRuntimeClass(rtClass)
      }
      case _                   => ()
    }
    conf
  }
  /**
   * Record as configuration properties the channel number to InputFormat mappings
   * as described by the DataSource
   */
  private def configureSourceInputFormat(source: Source) = (conf: Configuration) => {
    conf.addValues(INPUT_FORMAT_PROPERTY, source.id+";"+source.inputFormat.getName)
  }

  /**
   * Call the DataSource's configure method but with a proxy Job object. Then, add the
   * configuration properties added by the configure method to the main job's configuration
   * but prefixed with "scoobi.inputX", where X is the channel number.
   *
   * Also include the properties which might have been modified by the source (like the DistributedCache files)
   */
  private def configureSource(source: Source)(implicit sc: ScoobiConfiguration) = (conf: Configuration) => {
    val job = Compatibility.newJob(new Configuration(conf))
    source.inputConfigure(job)

    Option(job.getConfiguration.get(cache.CACHE_FILES)).foreach { files =>
      conf.addValues(cache.CACHE_FILES, files.split(",").toSeq:_*)
    }
    conf.updateWith(job.getConfiguration) { case (k, v) if k != cache.CACHE_FILES =>
      (ChannelPrefix.prefix(source.id, k), v)
    }
  }

  /**
   * Configure a new configuration object for a given channel by extracting the channel keys from the current context
   *
   * @return a new Configuration from an existing context (for the configuration) and a channel id
   */
  private def extractChannelConfiguration(configuration: Configuration, channel: Int): Configuration = {
    val Prefix = ChannelPrefix.regex(channel)

    new Configuration(configuration).updateWith { case (Prefix(k), v) =>
      (k, v)
    }
  }

  /** Get a map of all the input formats per channel id. */
  private def getInputFormats(context: JobContext): Map[Int, (InputFormat[_,_], Configuration)] = {
    val conf = Compatibility.getConfiguration(context)
    val Entry = """(.*);(.*)""".r

    conf.get(INPUT_FORMAT_PROPERTY).split(",").toList.map {
      case Entry(ch, infmt) => {
        val configuration = extractChannelConfiguration(conf, ch.toInt)
        (ch.toInt, (ReflectionUtils.newInstance(conf.getClassLoader.loadClass(infmt), configuration).asInstanceOf[InputFormat[_,_]], configuration))
      }
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
    val inputFormat = ReflectionUtils.newInstance(taggedInputSplit.inputFormatClass, context.getConfiguration).asInstanceOf[InputFormat[K, V]]
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
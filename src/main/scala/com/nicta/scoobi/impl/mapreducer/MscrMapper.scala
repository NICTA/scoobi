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

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.mapreduce.{Mapper => HMapper, TaskInputOutputContext, MapContext}

import core._
import rtt._
import util.DistCache
import com.nicta.scoobi.impl.plan.mscr.{OutputChannels, InputChannels, InputChannel}
import reflect.ClasspathDiagnostics
import org.apache.hadoop.io.{WritableComparable, WritableComparator, NullWritable}
import scala.collection.mutable
import org.apache.hadoop.util.ReflectionUtils
import com.nicta.scoobi.impl.exec.ConfiguredWritableComparator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

/**
 * Hadoop Mapper class for an MSCR
 *
 * It is composed of several tagged mappers which are taking inputs of a given type on a channel and emitting the result
 * for different tagged outputs
 */
class MscrMapper extends HMapper[Any, Any, TaggedKey, TaggedValue] {

  lazy implicit val logger = LogFactory.getLog("scoobi.MapTask")
  private var allInputChannels: InputChannels = _
  private var allOutputChannels: OutputChannels = _
  private var channelOutputFormat: ChannelOutputFormat = _
  private var inMemoryContext: InMemoryInputOutputContext = _
  private var taggedInputChannels: Seq[InputChannel] = _
  private var tk: TaggedKey = _
  private var tv: TaggedValue = _
  private var hasReduceTasks = true

  private var countValuesPerMapper = false
  private var mapperFunction = bareMapperFunction
  private val bareMapperFunction = (key: Any, value: Any, context: TaskInputOutputContext[Any, Any, Any, Any]) =>
    taggedInputChannels.foreach(channel => channel.map(key, value, new InputOutputContext(context)))
  private val countingMapperFunction = (key: Any, value: Any, context: TaskInputOutputContext[Any, Any, Any, Any]) => {
    context.getCounter(Configurations.MAPPER_VALUES_COUNTER, s"mapper-$mapperNumber").increment(1)
    taggedInputChannels.foreach(channel => channel.map(key, value, new InputOutputContext(context)))
  }
  private var mapperNumber = 0

  override def setup(context: HMapper[Any, Any, TaggedKey, TaggedValue]#Context) {
    ClasspathDiagnostics.logInfo
    val jobStep = ScoobiConfiguration(context.getConfiguration).jobStep

    allInputChannels = DistCache.pullObject[InputChannels](context.getConfiguration, s"scoobi.mappers-$jobStep").getOrElse(InputChannels(Seq()))
    tk = ReflectionUtils.newInstance(context.getMapOutputKeyClass  , context.getConfiguration).asInstanceOf[TaggedKey]
    tv = ReflectionUtils.newInstance(context.getMapOutputValueClass, context.getConfiguration).asInstanceOf[TaggedValue]

    val inputSplit = context.getInputSplit.asInstanceOf[TaggedInputSplit]
    val mapContext = context.asInstanceOf[MapContext[Any,Any,Any,Any]]
    logger.info("Starting on " + java.net.InetAddress.getLocalHost.getHostName)
    logger.info("Input is " + inputSplit)
    taggedInputChannels = allInputChannels.channelsForSource(inputSplit.channel)

    // if there are no reducers for this job use an in-memory context to hold the mapped values and pass them directly to output channels
    if (context.getNumReduceTasks == 0) {
      hasReduceTasks = false
      inMemoryContext = new InMemoryInputOutputContext(mapContext)
      taggedInputChannels.foreach(_.setup(inMemoryContext))

      channelOutputFormat = new ChannelOutputFormat(context)
      allOutputChannels = DistCache.pullObject[OutputChannels](context.getConfiguration, s"scoobi.reducers-$jobStep").getOrElse(OutputChannels(Seq()))
      allOutputChannels.setup(channelOutputFormat)(context.getConfiguration)
    } else taggedInputChannels.foreach(_.setup(new InputOutputContext(mapContext)))

    countValuesPerMapper = context.getConfiguration.getBoolean(Configurations.COUNT_MAPPER_VALUES, false)
    mapperNumber         = context.getTaskAttemptID.getTaskID.getId

    if (countValuesPerMapper) mapperFunction = countingMapperFunction
  }

  override def map(key: Any, value: Any, context: HMapper[Any, Any, TaggedKey, TaggedValue]#Context) {
    val taskContext = context.asInstanceOf[TaskInputOutputContext[Any, Any, Any, Any]]

    mapperFunction.apply(key, value, taskContext)

    // if there are no reducers pass the mapped value of a given tag to the corresponding output channel
    if (!hasReduceTasks) {
      taggedInputChannels.foreach { channel =>
        channel.tags.foreach { tag =>
          allOutputChannels.channel(tag).foreach { outputChannel =>
            inMemoryContext.getValues(tag).foreach(values => outputChannel.reduce(NullWritable.get, values, channelOutputFormat)(context.getConfiguration))
          }
        }
      }
      inMemoryContext.clear
    }
  }

  override def cleanup(context: HMapper[Any, Any, TaggedKey, TaggedValue]#Context) {
    if (context.getNumReduceTasks == 0) {
      allOutputChannels.cleanup(channelOutputFormat)(context.getConfiguration)
      channelOutputFormat.close
    }
    taggedInputChannels.foreach(_.cleanup(new InputOutputContext(context.asInstanceOf[MapContext[Any,Any,Any,Any]])))
  }
}

import scalaz.Scalaz._
import scalaz.std.vector.vectorSyntax._

/**
 * This context holds values in memory, by tag, in order to pass them later on to output channels for writing
 */
class InMemoryInputOutputContext(context: TaskInputOutputContext[Any,Any,Any,Any]) extends InputOutputContext(context: TaskInputOutputContext[Any,Any,Any,Any]) {
  var values: Map[Int, Vector[Any]] = Map[Int, Vector[Any]]()

  def getValues(tag: Int): Option[Vector[Any]] = values.get(tag)

  def clear = { values = Map[Int, Vector[Any]]() }

  override def write(k: Any, v: Any) {
    val taggedValue = v.asInstanceOf[TaggedValue]
    values = values |+| Map(taggedValue.tag -> Vector(taggedValue.get(taggedValue.tag)))
  }
}
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
import org.apache.hadoop.mapreduce.{Mapper => HMapper, Counter, TaskInputOutputContext, MapContext}

import core._
import rtt._
import util.DistCache
import com.nicta.scoobi.impl.plan.mscr.{OutputChannel, OutputChannels, InputChannels, InputChannel}
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
  private var compositeContext: CompositeInputOutputContext = _
  private var taggedInputChannels: Seq[InputChannel] = _
  private var tk: TaggedKey = _
  private var tv: TaggedValue = _
  private var hasBypassOutput = false

  private var countValuesPerMapper = false
  private var counter: Counter = _

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

    // if there are bypass nodes in the channels, use a composite context
    if (taggedInputChannels.exists(_.bypassOutputNodes.nonEmpty)) {
      hasBypassOutput = true
      allOutputChannels = DistCache.pullObject[OutputChannels](context.getConfiguration, s"scoobi.reducers-$jobStep").getOrElse(OutputChannels(Seq()))
      channelOutputFormat = new ChannelOutputFormat(context)

      val bypassTags = taggedInputChannels.flatMap(_.bypassTags)
      compositeContext = new CompositeInputOutputContext(mapContext, OutputChannels(bypassTags.flatMap(allOutputChannels.channel)), channelOutputFormat)
      taggedInputChannels.foreach(_.setup(compositeContext))

      allOutputChannels.setup(channelOutputFormat)(context.getConfiguration)
    } else taggedInputChannels.foreach(_.setup(new InputOutputContext(mapContext)))

    countValuesPerMapper = context.getConfiguration.getBoolean(Configurations.COUNT_MAPPER_VALUES, false)

    if (countValuesPerMapper) {
      val mapperNumber = context.getTaskAttemptID.getTaskID.getId
      counter = context.getCounter(Configurations.MAPPER_VALUES_COUNTER, s"mapper-$mapperNumber")
    }
  }

  override def map(key: Any, value: Any, context: HMapper[Any, Any, TaggedKey, TaggedValue]#Context) {
    val taskContext = context.asInstanceOf[TaskInputOutputContext[Any, Any, Any, Any]]

    if (countValuesPerMapper) counter.increment(1)
    if (hasBypassOutput)
      taggedInputChannels.foreach(channel => channel.map(key, value, compositeContext))
    else
      taggedInputChannels.foreach(channel => channel.map(key, value, new InputOutputContext(taskContext)))
  }

  override def cleanup(context: HMapper[Any, Any, TaggedKey, TaggedValue]#Context) {
    if (hasBypassOutput) {
      allOutputChannels.cleanup(channelOutputFormat)(context.getConfiguration)
      channelOutputFormat.close
    }
    taggedInputChannels.foreach(_.cleanup(new InputOutputContext(context.asInstanceOf[MapContext[Any,Any,Any,Any]])))
  }
}

/**
 * This context either writes to the hadoop context or to an output channel depending on the value tag
 * 
 * If the result of the map operation must go to the reducer, use the normal hadoop context
 * if it must go to an output file directly (i.e. to a "bypass" output tag), use the corresponding output channel
 * as a reducer
 * 
 */
class CompositeInputOutputContext(context: TaskInputOutputContext[Any,Any,Any,Any], outputChannels: OutputChannels, channelOutputFormat: ChannelOutputFormat) extends InputOutputContext(context: TaskInputOutputContext[Any,Any,Any,Any]) {
  override def write(k: Any, v: Any) {
    val taggedValue = v.asInstanceOf[TaggedValue]
    outputChannels.channel(taggedValue.tag).map(_.reduce(k, Seq(taggedValue.get(taggedValue.tag)), channelOutputFormat)(context.getConfiguration)).
      getOrElse(context.write(k, v))
  }
}
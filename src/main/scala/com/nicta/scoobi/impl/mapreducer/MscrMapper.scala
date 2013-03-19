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
import org.apache.hadoop.mapreduce.{Mapper => HMapper, MapContext, TaskInputOutputContext}

import core._
import rtt._
import util.DistCache
import plan.mscr.{InputChannels, InputChannel}

/**
 * Hadoop Mapper class for an MSCR
 *
 * It is composed of several tagged mappers which are taking inputs of a given type on a channel and emitting the result
 * for different tagged outputs
 */
class MscrMapper extends HMapper[Any, Any, TaggedKey, TaggedValue] {

  lazy val logger = LogFactory.getLog("scoobi.MapTask")
  private var allInputChannels: InputChannels = _
  private var taggedInputChannels: Seq[InputChannel] = _
  private var tk: TaggedKey = _
  private var tv: TaggedValue = _

  override def setup(context: HMapper[Any, Any, TaggedKey, TaggedValue]#Context) {

    allInputChannels = DistCache.pullObject[InputChannels](context.getConfiguration, "scoobi.mappers").getOrElse(InputChannels(Seq()))
    tk = context.getMapOutputKeyClass.newInstance.asInstanceOf[TaggedKey]
    tv = context.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]
    tk.configuration = context.getConfiguration
    tv.configuration = context.getConfiguration

    val inputSplit = context.getInputSplit.asInstanceOf[TaggedInputSplit]
    logger.info("Starting on " + java.net.InetAddress.getLocalHost.getHostName)
    logger.info("Input is " + inputSplit)
    taggedInputChannels = allInputChannels.channelsForSource(inputSplit.channel)
    taggedInputChannels.foreach(_.setup(InputOutputContext(context.asInstanceOf[MapContext[Any,Any,Any,Any]])))

  }

  override def map(key: Any, value: Any, context: HMapper[Any, Any, TaggedKey, TaggedValue]#Context) {
    taggedInputChannels.foreach(_.map(key, value, InputOutputContext(context.asInstanceOf[MapContext[Any,Any,Any,Any]])))
  }

  override def cleanup(context: HMapper[Any, Any, TaggedKey, TaggedValue]#Context) {
    taggedInputChannels.foreach(_.cleanup(InputOutputContext(context.asInstanceOf[MapContext[Any,Any,Any,Any]])))
  }
}

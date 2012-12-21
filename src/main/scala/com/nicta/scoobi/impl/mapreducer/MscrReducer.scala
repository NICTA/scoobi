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
import org.apache.hadoop.mapreduce.{Reducer => HReducer}
import scala.collection.JavaConversions._

import core._
import rtt._
import util.DistCache
import plan.mscr.OutputChannels

/** Hadoop Reducer class for an MSCR. */
class MscrReducer[K2, V2, B, E, K3, V3] extends HReducer[TaggedKey, TaggedValue, K3, V3] {

  lazy val logger = LogFactory.getLog("scoobi.ReduceTask")

  private var outputChannels: OutputChannels = _
  private var channelOutput: ChannelOutputFormat = _

  override def setup(context: HReducer[TaggedKey, TaggedValue, K3, V3]#Context) {
    outputChannels = DistCache.pullObject[OutputChannels](context.getConfiguration, "scoobi.reducers").getOrElse(Map())
    channelOutput = new ChannelOutputFormat(context)

    logger.info("Starting on " + java.net.InetAddress.getLocalHost.getHostName)
    outputChannels.setup(context.getConfiguration)
  }

  override def reduce(key: TaggedKey,
                      values: java.lang.Iterable[TaggedValue],
                      context: HReducer[TaggedKey, TaggedValue, K3, V3]#Context) {

    /* Get the right output value type and output directory for the current channel,
     * specified by the key's tag. */
    val outputChannel = outputChannels.channel(key.tag)
    /* Convert java.util.Iterable[TaggedValue] to Iterable[V2]. */
    val untaggedValues = new UntaggedValues[V2](key.tag, values)
    outputChannel.reduce(key, untaggedValues, channelOutput)(configuration)
  }

  override def cleanup(context: HReducer[TaggedKey, TaggedValue, K3, V3]#Context) {
    outputChannels.cleanup(context)
    channelOutput.close()
  }
}

case class UntaggedValues[T](tag: Int, values: java.lang.Iterable[TaggedValue]) extends Iterable[T] {
  lazy val iterator = values.iterator map (_.get(tag).asInstanceOf[T])
}

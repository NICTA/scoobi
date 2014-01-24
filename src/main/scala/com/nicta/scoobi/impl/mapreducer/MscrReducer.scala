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
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.impl.exec.ConfiguredWritableComparator
import org.apache.hadoop.mapred.JobConf

/** Hadoop Reducer class for an MSCR. */
class MscrReducer extends HReducer[TaggedKey, TaggedValue, Any, Any] {

  lazy val logger = LogFactory.getLog("scoobi.ReduceTask")

  private var outputChannels: OutputChannels = _
  private var channelOutput: ChannelOutputFormat = _
  private var countValuesPerReducer: Boolean = false
  private var reducerNumber = 0

  override def setup(context: HReducer[TaggedKey, TaggedValue, Any, Any]#Context) {
    val configuration = context.getConfiguration
    outputChannels = DistCache.pullObject[OutputChannels](configuration, s"scoobi.reducers-${ScoobiConfiguration(configuration).jobStep}").getOrElse(OutputChannels(Seq()))
    channelOutput = new ChannelOutputFormat(context)

    logger.info("Starting on " + java.net.InetAddress.getLocalHost.getHostName)
    outputChannels.setup(channelOutput)(context.getConfiguration)

    countValuesPerReducer = context.getConfiguration.getBoolean(Configurations.COUNT_REDUCER_VALUES, false)
    reducerNumber = context.getTaskAttemptID.getTaskID.getId
  }

  override def reduce(key: TaggedKey, values: java.lang.Iterable[TaggedValue], context: HReducer[TaggedKey, TaggedValue, Any, Any]#Context) {
    var valuesNumber = 0

    /* Get the right output value type and output directory for the current channel,
     * specified by the key's tag. */
    outputChannels.channel(key.tag) foreach { channel =>
      val iterable = if (countValuesPerReducer) {
        new java.lang.Iterable[TaggedValue] {
          private val valuesIterator = values.iterator
          def iterator = new Iterator[TaggedValue] {
            def next = { valuesNumber += 1; valuesIterator.next }
            def hasNext = valuesIterator.hasNext
          }
        }
      } else values

      /* Convert java.util.Iterable[TaggedValue] to Iterable[V2]. */
      val untaggedValues =
      channel.reduce(key.get(key.tag), new UntaggedValues(key.tag, iterable), channelOutput)(context.getConfiguration)

      if (countValuesPerReducer) {
        context.getCounter(Configurations.REDUCER_VALUES_COUNTER, s"reducer-$reducerNumber").increment(valuesNumber)
      }
    }
  }

  override def cleanup(context: HReducer[TaggedKey, TaggedValue, Any, Any]#Context) {
    outputChannels.cleanup(channelOutput)(context.getConfiguration)
    channelOutput.close()
  }
}

case class UntaggedValues(tag: Int, values: java.lang.Iterable[TaggedValue]) extends Iterable[Any] {
  def iterator = values.iterator map (_.get(tag))
}

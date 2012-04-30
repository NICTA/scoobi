/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.impl.exec

import org.apache.hadoop.mapreduce.{Reducer => HReducer}
import scala.collection.JavaConversions._

import com.nicta.scoobi.Emitter
import com.nicta.scoobi.io.OutputConverter
import com.nicta.scoobi.impl.rtt.TaggedKey
import com.nicta.scoobi.impl.rtt.TaggedValue


/** Hadoop Reducer class for an MSCR. */
class MscrReducer[K2, V2, B, K3, V3] extends HReducer[TaggedKey, TaggedValue, K3, V3] {

  private type Reducers = Map[Int, (List[(Int, OutputConverter[_,_,_])], TaggedReducer[_,_,_])]
  private var outputs: Reducers = _
  private var channelOutput: ChannelOutputFormat = _

  override def setup(context: HReducer[TaggedKey, TaggedValue, K3, V3]#Context) = {
    outputs = DistCache.pullObject[Reducers](context.getConfiguration, "scoobi.reducers").getOrElse(Map())
    channelOutput = new ChannelOutputFormat(context)
  }

  override def reduce(key: TaggedKey,
                      values: java.lang.Iterable[TaggedValue],
                      context: HReducer[TaggedKey, TaggedValue, K3, V3]#Context) = {

    /* Get the right output value type and output directory for the current channel,
     * specified by the key's tag. */
    val channel = key.tag
    val converters = outputs(channel)._1.asInstanceOf[List[(Int, OutputConverter[K3, V3, B])]]
    val reducer = outputs(channel)._2.asInstanceOf[TaggedReducer[K2, V2, B]]

    /* Convert java.util.Iterable[TaggedValue] to Iterable[V2]. */
    val untaggedValues = new Iterable[V2] { def iterator = values.iterator map (_.get(channel).asInstanceOf[V2]) }

    /* Do the reduction. */
    val emitter = new Emitter[B] {
      def emit(x: B) = converters foreach { case (ix, converter) => channelOutput.write(channel, ix, converter.toKeyValue(x)) }
    }
    reducer.reduce(key.get(channel).asInstanceOf[K2], untaggedValues, emitter)
  }

  override def cleanup(context: HReducer[TaggedKey, TaggedValue, K3, V3]#Context) = {
    channelOutput.close()
  }
}

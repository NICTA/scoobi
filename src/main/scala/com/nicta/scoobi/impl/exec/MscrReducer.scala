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

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{Reducer => HReducer, _}

import com.nicta.scoobi.Emitter
import com.nicta.scoobi.impl.rtt.ScoobiWritable
import com.nicta.scoobi.impl.rtt.Tagged
import com.nicta.scoobi.impl.rtt.TaggedKey
import com.nicta.scoobi.impl.rtt.TaggedValue


/** Hadoop Reducer class for an MSCR. */
class MscrReducer[K, V, B] extends HReducer[TaggedKey, TaggedValue, NullWritable, ScoobiWritable[B]] {

  private var outputs: Map[Int, (Int, TaggedReducer[_,_,_])] = _
  private var channelOutput: ChannelOutputFormat[ScoobiWritable[B]] = _

  override def setup(context: HReducer[TaggedKey, TaggedValue, NullWritable, ScoobiWritable[B]]#Context) = {
    outputs = DistCache.pullObject(context.getConfiguration, "scoobi.output.reducers").asInstanceOf[Map[Int, (Int, TaggedReducer[_,_,_])]]
    channelOutput = new ChannelOutputFormat(context)
  }

  override def reduce(key: TaggedKey,
                      values: java.lang.Iterable[TaggedValue],
                      context: HReducer[TaggedKey, TaggedValue, NullWritable, ScoobiWritable[B]]#Context) = {

    val tag = key.tag
    val numOutputs = outputs(tag)._1

    /* Get the right output value type and output directory for the current channel,
     * specified by the key's tag. */
    val reducer = outputs(tag)._2.asInstanceOf[TaggedReducer[K, V, B]]

    val v = ChannelOutputFormat.getValueClass(context, tag).newInstance.asInstanceOf[ScoobiWritable[B]]

    /* Convert Iterator[TaggedValue] to Iterable[V]. */
    val valuesStream = Stream.continually(if (values.iterator.hasNext) values.iterator.next else null).takeWhile(_ != null)
    val untaggedValues = valuesStream.map(_.get(tag).asInstanceOf[V]).toIterable

    /* Do the reduction. */
    val emitter = new Emitter[B] {
      def emit(x: B) = {
        v.set(x)
        channelOutput.write(tag, numOutputs, v)
      }
    }
    reducer.reduce(key.get(tag).asInstanceOf[K], untaggedValues, emitter)
  }

  override def cleanup(context: HReducer[TaggedKey, TaggedValue, NullWritable, ScoobiWritable[B]]#Context) = {
    channelOutput.close()
  }
}

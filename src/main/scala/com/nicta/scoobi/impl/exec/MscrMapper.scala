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
import org.apache.hadoop.mapreduce.{Mapper => HMapper, _}

import com.nicta.scoobi.Emitter
import com.nicta.scoobi.impl.rtt.ScoobiWritable
import com.nicta.scoobi.impl.rtt.Tagged
import com.nicta.scoobi.impl.rtt.TaggedKey
import com.nicta.scoobi.impl.rtt.TaggedValue


/** Hadoop Mapper class for an MSCR. */
class MscrMapper[A, K, V] extends HMapper[NullWritable, ScoobiWritable[A], TaggedKey, TaggedValue] {

  var inputs: Map[Int, Set[TaggedMapper[_,_,_]]] = _
  var mappers: Set[TaggedMapper[A, K, V]] = _
  var tk: TaggedKey = _
  var tv: TaggedValue = _


  override def setup(context: HMapper[NullWritable, ScoobiWritable[A], TaggedKey, TaggedValue]#Context) = {
    inputs = DistCache.pullObject(context.getConfiguration, "scoobi.input.mappers").asInstanceOf[Map[Int, Set[TaggedMapper[_,_,_]]]]
    tk = context.getMapOutputKeyClass.newInstance.asInstanceOf[TaggedKey]
    tv = context.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]

    /* Find the mappers for this input channel from the tagged input split. */
    val inputSplit = context.getInputSplit.asInstanceOf[TaggedInputSplit]
    mappers = inputs(inputSplit.channel).asInstanceOf[Set[TaggedMapper[A, K, V]]]
    mappers.foreach { _.setup() }
  }

  override def map(key: NullWritable,
                   value: ScoobiWritable[A],
                   context: HMapper[NullWritable, ScoobiWritable[A], TaggedKey, TaggedValue]#Context) = {

    mappers foreach { mapper =>
      val emitter = new Emitter[(K, V)] {
        def emit(x: (K, V)) = {
          mapper.tags.foreach { tag =>
            tk.set(tag, x._1)
            tv.set(tag, x._2)
            context.write(tk, tv)
          }
        }
      }
      mapper.map(value.get.asInstanceOf[A], emitter)
    }
  }

  override def cleanup(context: HMapper[NullWritable, ScoobiWritable[A], TaggedKey, TaggedValue]#Context) = {

    mappers foreach { mapper =>
      val emitter = new Emitter[(K, V)] {
        def emit(x: (K, V)) = {
          mapper.tags.foreach { tag =>
            tk.set(tag, x._1)
            tv.set(tag, x._2)
            context.write(tk, tv)
          }
        }
      }
      mapper.cleanup(emitter)
    }
  }
}

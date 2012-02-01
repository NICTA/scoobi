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

import org.apache.hadoop.mapreduce.{Mapper => HMapper, _}

import com.nicta.scoobi.Emitter
import com.nicta.scoobi.io.InputConverter
import com.nicta.scoobi.impl.rtt.Tagged
import com.nicta.scoobi.impl.rtt.TaggedKey
import com.nicta.scoobi.impl.rtt.TaggedValue


/** Hadoop Mapper class for an MSCR. */
class MscrMapper[K1, V1, A, K2, V2] extends HMapper[K1, V1, TaggedKey, TaggedValue] {

  private var inputs: Map[Int, (InputConverter[K1, V1, A], Set[TaggedMapper[A, _, _]])] = _
  private var converter: InputConverter[K1, V1, A] = _
  private var mappers: Set[TaggedMapper[A, K2, V2]] = _
  private var tk: TaggedKey = _
  private var tv: TaggedValue = _

  override def setup(context: HMapper[K1, V1, TaggedKey, TaggedValue]#Context) = {

    tk = context.getMapOutputKeyClass.newInstance.asInstanceOf[TaggedKey]
    tv = context.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]

    /* Find the converter and its mappers for this input channel from the tagged input split. */
    inputs = DistCache.pullObject(context.getConfiguration, "scoobi.mappers")
                      .asInstanceOf[Map[Int, (InputConverter[K1, V1, A], Set[TaggedMapper[A,_,_]])]]
    val inputSplit = context.getInputSplit.asInstanceOf[TaggedInputSplit]
    val input = inputs(inputSplit.channel)

    converter = input._1.asInstanceOf[InputConverter[K1, V1, A]]
    mappers = input._2.asInstanceOf[Set[TaggedMapper[A, K2, V2]]]

    mappers.foreach { _.setup() }
  }

  override def map(key: K1, value: V1, context: HMapper[K1, V1, TaggedKey, TaggedValue]#Context) = {
    val v: A = converter.fromKeyValue(key, value).asInstanceOf[A]
    mappers foreach { mapper =>
      val emitter = new Emitter[(K2, V2)] {
        def emit(x: (K2, V2)) = {
          mapper.tags.foreach { tag =>
            tk.set(tag, x._1)
            tv.set(tag, x._2)
            context.write(tk, tv)
          }
        }
      }
      mapper.map(v, emitter)
    }
  }

  override def cleanup(context: HMapper[K1, V1, TaggedKey, TaggedValue]#Context) = {
    mappers foreach { mapper =>
      val emitter = new Emitter[(K2, V2)] {
        def emit(x: (K2, V2)) = {
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

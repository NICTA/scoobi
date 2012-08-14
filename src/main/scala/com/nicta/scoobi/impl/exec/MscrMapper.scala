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
package exec

import org.apache.hadoop.mapreduce.{Mapper => HMapper}

import core._
import io.InputConverter
import rtt._

/** Hadoop Mapper class for an MSCR. */
class MscrMapper[K1, V1, A, E, K2, V2] extends HMapper[K1, V1, TaggedKey, TaggedValue] {

  private type Mappers = Map[Int, (InputConverter[K1, V1, A], Set[(Env[_], TaggedMapper[A, _, _, _])])]
  private var inputs: Mappers = _
  private var converter: InputConverter[K1, V1, A] = _
  private var mappers: Set[(_, TaggedMapper[A, _, _, _])] = _
  private var tk: TaggedKey = _
  private var tv: TaggedValue = _

  override def setup(context: HMapper[K1, V1, TaggedKey, TaggedValue]#Context) = {

    tk = context.getMapOutputKeyClass.newInstance.asInstanceOf[TaggedKey]
    tv = context.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]

    /* Find the converter and its mappers for this input channel from the tagged input split. */
    inputs = DistCache.pullObject[Mappers](context.getConfiguration, "scoobi.mappers").getOrElse(Map())
    val inputSplit = context.getInputSplit.asInstanceOf[TaggedInputSplit]
    val input: (InputConverter[K1, V1, A], Set[(Env[_], TaggedMapper[A, _, _, _])]) = inputs(inputSplit.channel)

    converter = input._1.asInstanceOf[InputConverter[K1, V1, A]]

    mappers = input._2 map { case (env, mapper) => (env.pull(context.getConfiguration), mapper) }

    mappers.foreach { case (env, mapper: TaggedMapper[_, _, _, _]) =>
      mapper.setup(env)
    }
  }

  override def map(key: K1, value: V1, context: HMapper[K1, V1, TaggedKey, TaggedValue]#Context) = {
    val v: A = converter.fromKeyValue(context, key, value).asInstanceOf[A]
    mappers foreach { case (env, mapper: TaggedMapper[_, _, _, _]) =>
      val emitter = new Emitter[(K2, V2)] {
        def emit(x: (K2, V2)) = {
          mapper.tags.foreach { tag =>
            tk.set(tag, x._1)
            tv.set(tag, x._2)
            context.write(tk, tv)
          }
        }
      }
      mapper.map(env, v, emitter)
    }
  }

  override def cleanup(context: HMapper[K1, V1, TaggedKey, TaggedValue]#Context) = {
    mappers foreach { case (env, mapper: TaggedMapper[_, _, _, _]) =>
      val emitter = new Emitter[(K2, V2)] {
        def emit(x: (K2, V2)) = {
          mapper.tags.foreach { tag =>
            tk.set(tag, x._1)
            tv.set(tag, x._2)
            context.write(tk, tv)
          }
        }
      }
      mapper.cleanup(env, emitter)
    }
  }
}

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
package com.nicta.scoobi

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{Mapper => HMapper, _}


/** Hadoop Mapper class for an MSCR. */
class MscrMapper[A, K, V] extends HMapper[NullWritable, ScoobiWritable[A], TaggedKey, TaggedValue] {

  var inputs: Map[Int, Set[TaggedMapper[_,_,_]]] = _
  var mappers: Set[TaggedMapper[A, K, V]] = _
  var tk: TaggedKey = _
  var tv: TaggedValue = _


  def configure(conf: JobConf) = {
    inputs = DistributedObject.pullObject(conf, "scoobi.input.mappers").asInstanceOf[Map[Int, Set[TaggedMapper[_,_,_]]]]
    mappers = null
    tk = conf.getMapOutputKeyClass.newInstance.asInstanceOf[TaggedKey]
    tv = conf.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]
  }

  def map(key: NullWritable,
          value: ScoobiWritable[A],
          output: OutputCollector[TaggedKey, TaggedValue],
          reporter: Reporter) = {

    /* Find the mappers for this input channel from the tagged input split. */
    if (mappers == null) {
      val inputSplit = reporter.getInputSplit.asInstanceOf[TaggedInputSplit]
      mappers = inputs(inputSplit.channel).asInstanceOf[Set[TaggedMapper[A, K, V]]]
    }

    /* Do the mappings. */
    mappers.foreach { mapper =>
      mapper.map(value.get.asInstanceOf[A]).foreach { case (k, v) =>
        mapper.tags.foreach { tag =>
          tk.set(tag, k)
          tv.set(tag, v)
          output.collect(tk, tv)
        }
      }
    }
  }

  def close() = {
  }
}

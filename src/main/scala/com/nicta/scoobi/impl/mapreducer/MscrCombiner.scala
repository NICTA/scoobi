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

import com.nicta.scoobi.impl.ScoobiConfiguration
import com.nicta.scoobi.impl.plan.comp.Combine
import org.apache.hadoop.mapreduce.{Reducer => HReducer, TaskInputOutputContext}
import scala.collection.JavaConversions._

import rtt._
import util.DistCache
import plan.mscr.{OutputChannel, OutputChannels}
import plan.comp.Combine
import com.nicta.scoobi.core._
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.impl.exec.ConfiguredWritableComparator
import org.apache.hadoop.mapred.JobConf

/** Hadoop Combiner class for an MSCR. */
class MscrCombiner extends HReducer[TaggedKey, TaggedValue, TaggedKey, TaggedValue] {

  private type Combiners = Map[Int, Combine]
  private var combiners: Combiners = _
  private var tv: TaggedValue = _

  override def setup(context: HReducer[TaggedKey, TaggedValue, TaggedKey, TaggedValue]#Context) {
    val jobStep = ScoobiConfiguration(context.getConfiguration).jobStep

    // the combiner setup function might be called several times
    // this is why we memoise the deserialised combiners to improve performances
    combiners = DistCache.pullObject[Combiners](context.getConfiguration, s"scoobi.combiners-$jobStep", memoise = true).getOrElse(Map())
    tv = ReflectionUtils.newInstance(context.getMapOutputValueClass, context.getConfiguration).asInstanceOf[TaggedValue]
  }

  override def reduce(key: TaggedKey, values: java.lang.Iterable[TaggedValue], context: HReducer[TaggedKey, TaggedValue, TaggedKey, TaggedValue]#Context) {
    val outerContext = context
    val tag = key.tag

    if (combiners.contains(tag)) {
      /* Only perform combining if one is available for this tag. */
      val combiner = combiners(tag)

      /* Convert java.util.Iterable[TaggedValue] to Iterable[V2]. */
      val untaggedValues = new Iterable[Any] { def iterator = values.iterator map (_.get(tag)) }

      /* Do the combining. */
      val emitter = new EmitterWriter with InputOutputContextScoobiJobContext {
        def write(value: Any): Unit = {
          tv.setTag(tag)
          tv.set(value)
          context.write(key, tv)
        }
        def context: InputOutputContext = new InputOutputContext(outerContext.asInstanceOf[TaskInputOutputContext[Any,Any,Any,Any]])
      }
      combiner.combine(untaggedValues, emitter)
    } else
      /* If no combiner for this tag, TK-TV passes through. */
      values.foreach { value => context.write(key, value) }
  }

  override def cleanup(context: HReducer[TaggedKey, TaggedValue, TaggedKey, TaggedValue]#Context) { }
}

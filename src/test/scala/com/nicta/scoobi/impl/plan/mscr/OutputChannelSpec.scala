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
package plan
package mscr

import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer
import org.specs2.specification.Grouped
import org.specs2.matcher.ThrownExpectations
import testing.script.UnitSpecification
import comp._
import core._
import mapreducer._

class OutputChannelSpec extends UnitSpecification with Grouped { def is = s2"""

 An OutputChannel encapsulates processing nodes for the outputs of a Mscr.
 There are 2 types of channels:

   - a GbkOutputChannel which groups values per key (with a GroupByKey node)
     and possibly combine and / or reduce them

   - a BypassOutputChannel which simply copies the values of a previous InputChannel
     to some sinks

Tag
====
 An output channel defines a tag which is a unique id identifying the node which receives the input key/values
   + this node is a GroupByKey for a GbkOutputChannel and a ParallelDo for a BypassOutputChannel
   + the output channel tag is used to determine if 2 channels are equal

Inputs/Outputs
==============

 The "lastNode" of a GbkOutputChannel is the last node of that channel which will emit values
   + it is the reducer if it exists or else the combiner or else the groupByKey
   + it is the input of a BypassOutputChannel

 the sinks of an OutputChannel are
   + the bridgeStore related to the last node
   + the additional sinks of this last node

Processing
==========

 Processing key / values
   + gbk only
   + gbk + combiner
   + gbk + reducer
   + gbk + combiner + reducer
   + bypass
                                                                                                                        """

  "generalities" - new group with factory {
    val (gbk1, gbk2) = (gbk(load), gbk(StringSink()))

    eg := GbkOutputChannel(gbk1).tag === gbk1.id
    eg := {
      GbkOutputChannel(gbk1) === GbkOutputChannel(gbk1)
      GbkOutputChannel(gbk1) !=== GbkOutputChannel(gbk2)
    }
  }

  "inputs-outputs" - new group with factory with ThrownExpectations {
    val (gbk1, gbk2, pd1) = (gbk(load), gbk(StringSink()), pd(load))

    eg := {
      val cb1 = cb(gbk1)
      val red1 = pd(cb1)
      GbkOutputChannel(gbk1).lastNode                                             === gbk1
      GbkOutputChannel(gbk1, combiner = Some(cb1)).lastNode                       === cb1
      GbkOutputChannel(gbk1, reducer = Some(red1)).lastNode                       === red1
      GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(red1)).lastNode === red1
    }
    eg := BypassOutputChannel(pd1).lastNode === pd1

    eg := GbkOutputChannel(gbk1).sinks === Seq(gbk1.bridgeStore)
    eg := GbkOutputChannel(gbk2).sinks === Seq(gbk2.bridgeStore, StringSink())
  }

  "processing" - new group with factory {
    val (gbk1, gbk2) = (gbk(load), gbk(StringSink()))

    eg := {
      val channel = gbkChannel(gbk1)
      channel.reduce("1", Seq(1, 2, 3)) === Seq(("1", Seq(1, 2, 3)))
    }
    eg := {
      val channel = gbkChannel(gbk1, combiner = Some(cb(gbk1)))
      channel.reduce("1", Seq(1, 2, 3)) === Seq(("1", "123"))
    }
    eg := {
      val channel = gbkChannel(gbk1, reducer = Some(pd(gbk1).copy(dofn = MapFunction { case (k, vs: Iterable[_]) => (k, vs.mkString+"a") })))
      channel.reduce("1", Seq(1, 2, 3)) === Seq(("1", "123a"))
    }
    eg := {
      val cb1 = cb(gbk1)
      val channel = gbkChannel(gbk1, combiner = Some(cb1), reducer = Some(pd(cb1).copy(dofn = MapFunction { case (k, vs) => (k, vs+"a") })))
      channel.reduce("1", Seq(1, 2, 3)) === Seq(("1", "123a"))
    }
    eg := {
      val channel = bypassChannel(pd(load))
      channel.reduce("1", Seq(1, 2, 3)) === Seq(("nokey", 1), ("nokey", 2), ("nokey", 3))
    }
  }

  trait MockOutputChannel extends MscrOutputChannel { outer =>

    override protected def scoobiConfiguration(configuration: Configuration) = ScoobiConfigurationImpl.unitEnv(configuration)
    override protected def convert(sink: Sink, value: Any)(implicit configuration: Configuration) = value match {
      case (k, v) => (k, v)
      case other  => ("nokey", other)
    }

    lazy val outputFormat = new ChannelOutputFormat(null) {
      val keyValues = new ListBuffer[Any]
      override def write[K, V](tag: Int, sinkId: Int, kv: (K, V)) {
        keyValues.append((kv._1, kv._2))
      }
      override def close() { }
    }
    setup(outputFormat)(new Configuration)

    def reduce(key: Any, values: Iterable[Any]): Seq[Any] = {
      outer.reduce(key, values, outputFormat)(new Configuration)
      outputFormat.keyValues.toList
    }
  }

  /** create a gbk output channel for a group by key, collecting the emitted results */
  def gbkChannel(gbk: GroupByKey, combiner: Option[Combine] = None, reducer: Option[ParallelDo] = None) =
    new GbkOutputChannel(gbk, combiner, reducer) with MockOutputChannel
  /** create a bypass output channel for a parallelDo */
  def bypassChannel(pd: ParallelDo) = new BypassOutputChannel(pd) with MockOutputChannel
}

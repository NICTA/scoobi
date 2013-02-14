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
import testing.UnitSpecification
import comp._
import core._
import mapreducer._

class OutputChannelSpec extends UnitSpecification with Grouped { def is =

  """
  An OutputChannel encapsulates processing nodes for the outputs of a Mscr.
  There are 2 types of channels:

    - a GbkOutputChannel which groups values per key (with a GroupByKey node)
      and possibly combine and / or reduce them

    - a BypassOutputChannel which simply copies the values of a previous InputChannel
      to some sinks

  """                                                                                                                   ^
                                                                                                                        p^
    "An output channel defines a tag which is a unique id identifying the node which receives the input key/values"     ! g1.e1^
    "the output channel tag is used to determine if 2 channels are equal"                                               ! g1.e2^
                                                                                                                        p^
    "the sinks of an OutputChannel are"                                                                                 ^
      "the bridgeStore related to the last node of the channel"                                                         ! g2.e1^
      "the additional sinks of this last node"                                                                          ! g2.e2^
                                                                                                                        p^
    "Processing key / values"                                                                                           ^
      "gbk only"                                                                                                        ! g3.e1^
      "gbk + combiner"                                                                                                  ! g3.e2^
      "gbk + reducer"                                                                                                   ! g3.e3^
      "gbk + combiner + reducer"                                                                                        ! g3.e4^
      "bypass"                                                                                                          ! g3.e5^
                                                                                                                        end

  "generalities" - new g1 with factory {
    val (gbk1, gbk2) = (gbk(load), gbk(StringSink()))

    e1 := GbkOutputChannel(gbk1).tag === gbk1.id
    e2 := {
      GbkOutputChannel(gbk1) === GbkOutputChannel(gbk1)
      GbkOutputChannel(gbk1) !=== GbkOutputChannel(gbk2)
    }
  }

  "sinks" - new g2 with factory {
    val (gbk1, gbk2) = (gbk(load), gbk(StringSink()))

    e1 := GbkOutputChannel(gbk1).sinks === gbk1.bridgeStore.toSeq
    e2 := GbkOutputChannel(gbk2).sinks === (gbk2.bridgeStore.toSeq :+ StringSink())
  }

  "processing" - new g3 with factory {
    val (gbk1, gbk2) = (gbk(load), gbk(StringSink()))

    e1 := {
      val channel = gbkChannel(gbk1)
      channel.reduce("1", Seq(1, 2, 3)) === Seq(("1", Seq(1, 2, 3)))
    }
    e2 := {
      val channel = gbkChannel(gbk1, combiner = Some(cb(gbk1)))
      channel.reduce("1", Seq(1, 2, 3)) === Seq(("1", "123"))
    }
    e3 := {
      val channel = gbkChannel(gbk1, reducer = Some(pd(gbk1).copy(dofn = MapFunction { case (k, vs: Iterable[_]) => (k, vs.mkString+"a") })))
      channel.reduce("1", Seq(1, 2, 3)) === Seq(("1", "123a"))
    }
    e4 := {
      val cb1 = cb(gbk1)
      val channel = gbkChannel(gbk1, combiner = Some(cb1), reducer = Some(pd(cb1).copy(dofn = MapFunction { case (k, vs) => (k, vs+"a") })))
      channel.reduce("1", Seq(1, 2, 3)) === Seq(("1", "123a"))
    }
    e5 := {
      val channel = bypassChannel(pd(load))
      channel.reduce("1", Seq(1, 2, 3)) === Seq(("nokey", 1), ("nokey", 2), ("nokey", 3))
    }
  }

  trait MockOutputChannel extends MscrOutputChannel { outer =>

    override protected def scoobiConfiguration(configuration: Configuration) = ScoobiConfigurationImpl.unitEnv(configuration)
    override protected def convert(sink: Sink, value: Any) = value match {
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
  def gbkChannel(gbk: GroupByKey, combiner: Option[Combine] = None, reducer: Option[ParallelDo] = None) = new GbkOutputChannel(gbk, combiner, reducer) with MockOutputChannel
  /** create a bypass output channel for a parallelDo */
  def bypassChannel(pd: ParallelDo) = new BypassOutputChannel(pd) with MockOutputChannel
}

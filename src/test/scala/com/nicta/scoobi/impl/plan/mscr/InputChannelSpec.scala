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

import testing.UnitSpecification
import org.specs2.specification.Groups
import comp.{ParallelDo, GroupByKey, factory}
import org.specs2.matcher.{Matcher, ThrownExpectations}
import core.{EmitterWriter, MapFunction, InputOutputContext, CompNode}
import org.apache.hadoop.conf.Configuration
import rtt.{MetadataTaggedValue, MetadataTaggedKey, TaggedValue, TaggedKey}
import scala.collection.mutable.ListBuffer

class InputChannelSpec extends UnitSpecification with Groups with ThrownExpectations { def is = sequential ^
                                                                                                                        """
  An InputChannel encapsulates parallelDo nodes which have a common source node.
  There are 2 types of InputChannels:

   * Mapper: which connects to Gbks
   * Floating: which connects to other types of Nodes
                                                                                                                        """^
                                                                                                                        p^
  "2 input channels with 2 different source nodes must be different"                                                    ! g1().e1^
  "the source of the input channel is the Load source if the sourceNode is a Load"                                      ! g1().e2^
  "the source of the input channel is the BridgeStore source if the sourceNode is a ProcessNode"                        ! g1().e3^
  "the input nodes of the input channel are: the source node+the environments of all the channel mappers"               ! g1().e4^
  "an input channel defines tags which are all the gbks ids it relates to"                                              ! g1().e5^
  "an input channel defines the types of keys emitted by tag"                                                           ^
    "as the types of the gbks key for a GbkInputChannel"                                                                ! g2().e1^
    "as Int for a FloatingInputChannel"                                                                                 ! g3().e1^p^
  "an input channel defines the types of values emitted by tag"                                                         ^
    "as the types of the gbks values for a GbkInputChannel"                                                             ! g2().e2^
    "as the types of the last mappers for a FloatingInputChannel"                                                       ! g3().e2^
                                                                                                                        endp^
  "An input channel must map key/values based on the mappers connected to its source"                                   ^
    "if there is only one mapper"                                                                                       ! g4().e1^
    "if there are 2 independent mappers"                                                                                ! g4().e2^
    "if there are 2 consecutive mappers"                                                                                ! g4().e3^
    "if there are 3 mappers as a tree"                                                                                  ! g4().e4^
                                                                                                                        endp^
  "A gbk input channel has mappers defined by the parallelDos connected to its source"                                  ^
    "load -> pd1 -> gbk1 -> pd2 -> gbk2"                                                                                ! g5().e1^
    "load -> pd1 -> gbk1; pd1 -> gbk2"                                                                                  ! g5().e2^
    "load -> pd1 -> gbk1 -> pd2 -> gbk2; pd1 -> gbk2"                                                                   ! g5().e3^
                                                                                                                        endp^
  "A floating input channel has mappers defined by the parallelDos connected to its source"                             ^
    "load -> pd1 floating -> pd2 -> mat"                                                                                ! g6().e1^
    "load -> pd1 floating -> pd2 floating -> mat"                                                                       ! g6().e2^
    "pd1 floating -> pd2 -> mat"                                                                                        ! g6().e3^
    "pd1 floating -> pd2 floating -> mat"                                                                               ! g6().e4^
                                                                                                                        end

  "general properties of input channels" - new g1 with factory {
    val (l1, l2) = (load, load)
    val pd1 = pd(l1)
    val (pd2, gbk1) = (pd(pd1), gbk(pd1))
    val gbk2 = gbk(pd2)

    e1 := {
      gbkInputChannel(l1)  === gbkInputChannel(l1)
      gbkInputChannel(l1) !=== gbkInputChannel(l2)
    }

    e2 := { gbkInputChannel(l1).source === l1.source }
    e3 := { gbkInputChannel(gbk1).source === gbk1.bridgeStore.get }
    e4 := { gbkInputChannel(l1, Seq(gbk1, gbk2)).inputNodes === Seq(l1, pd1.env, pd2.env) }
    e5 := { gbkInputChannel(l1, Seq(gbk1, gbk2)).tags === Seq(gbk1.id, gbk2.id) }
  }
  "gbk input channels key/values" - new g2 with factory {
    val (l1, l2) = (load, load)
    val pd1 = pd(l1)
    val (pd2, gbk1) = (pd(pd1), gbk(pd1))
    val gbk2 = gbk(pd2)

    e1 := {
      gbkInputChannel(l1, Seq(gbk1, gbk2)).keyTypes.tags === Seq(gbk1.id, gbk2.id)
      gbkInputChannel(l1, Seq(gbk1, gbk2)).keyTypes.types.values.toList === Seq((gbk1.wfk, gbk1.gpk), (gbk2.wfk, gbk2.gpk))
    }
    e2 := {
      gbkInputChannel(l1, Seq(gbk1, gbk2)).valueTypes.tags === Seq(gbk1.id, gbk2.id)
      gbkInputChannel(l1, Seq(gbk1, gbk2)).valueTypes.types.values.toList === Seq(Tuple1(gbk1.wfv), Tuple1(gbk2.wfv))
    }
  }
  "floating input channels key/values" - new g3 with factory {
    val l1 = load
    val pd1 = pd(l1)
    val pd2 = pd(pd1)
    val mat1 = mt(pd2)

    e1 := {
      floatingInputChannel(l1).keyTypes.tags === Seq(pd2.id)
      floatingInputChannel(l1).keyTypes.types.values.toList.mkString(",") === "(Int,AllGrouping)"
    }
    e2 := {
      floatingInputChannel(l1).valueTypes.tags === Seq(pd2.id)
      floatingInputChannel(l1).valueTypes.types.values.toList === Seq(Tuple1(pd2.wf))
    }
  }

  "mapping values" - new g4 with factory {
    val (l1, l2) = (load, load)
    val pd1 = pd(l1).copy(dofn = MapFunction(_.toString.toUpperCase))

    e1 := {
      mt(pd1)
      val channel = floatingInputChannel(l1)
      channel.map("1", "start", channel.context)
      channel.context.key === 1
      channel.context.value === "START"
    }

    e2 := {
      val pd2 = pd(l1).copy(dofn = MapFunction(_.toString.toLowerCase))
      val (mt1, mt2) = (mt(pd1), mt(pd2))
      aRoot(mt1, mt2)

      val channel = floatingInputChannel(l1)
      channel.map("1", "stARt", channel.context)
      channel.context.keys must beDistinct
      channel.context.values === Seq("START", "start")
    }

    e3 := {
      val pd2 = pd(pd1).copy(dofn = MapFunction(_.toString+" now"))
      mt(pd2)

      val channel = floatingInputChannel(l1)
      channel.map("1", "stARt", channel.context)
      channel.context.values === Seq("START now")
    }

    e4 := {
      val pd2 = pd(pd1).copy(dofn = MapFunction(a => (a, a.toString+" now")))
      val pd3 = pd(pd1).copy(dofn = MapFunction(a => (a, a.toString+" later")))
      val (gbk1, gbk2) = (gbk(pd2), gbk(pd3))
      aRoot(gbk1, gbk2)

      val channel = gbkInputChannel(l1, Seq(gbk1, gbk2))
      channel.map("1", "stARt", channel.context)
      channel.context.values === Seq("START now", "START later")
    }
  }

  "mappers of gbk input channels" - new g5 with factory {
    e1 := {
      // load -> pd1 -> gbk1 -> pd2 -> gbk2
      val l1 = load
      val pd1 = pd(l1)
      val gbk1 = gbk(pd1)
      val pd2 = pd(gbk1)
      val gbk2 = gbk(pd2)
      gbkInputChannel(l1, Seq(gbk1)).mappers === Seq(pd1)
    }
    e2 := {
      // load -> pd1 -> gbk1 -> pd2 -> gbk2
      //          |  -> gbk2
      val l1 = load
      val pd1 = pd(l1)
      val (gbk1, gbk2) = (gbk(pd1), gbk(pd1))
      val pd2 = pd(gbk1)
      gbkInputChannel(l1, Seq(gbk1, gbk2)).mappers === Seq(pd1)
    }
    e3 := {
      // load -> pd1 -> gbk1 -> pd2
      //          |          -> pd2 -> gbk2
      val l1 = load
      val pd1 = pd(l1)
      val gbk1 = gbk(pd1)
      val pd2 = pd(gbk1, pd1)
      val gbk2 = gbk(pd2)
      gbkInputChannel(l1, Seq(gbk1)).mappers === Seq(pd1)
    }
  }

  "mappers of floating input channels" - new g6 with factory {

    e1 := {
      // load -> pd1 floating -> pd2 -> mat
      val l1 = load
      val pd1 = pd(l1)
      val pd2 = pd(pd1)
      val mat1 = mt(pd2)
      floatingInputChannel(l1).mappers === Seq(pd1, pd2)
    }
    e2 := {
      // load -> pd1 floating -> pd2 floating -> mat
      val l1 = load
      val pd1 = pd(l1)
      val pd2 = pdWithEnv(pd1, mt(gbk(load)))
      val mat1 = mt(pd2)
      floatingInputChannel(l1).mappers === Seq(pd1)
    }
    e3 := {
      // pd1 floating -> pd2 -> mat
      val pd1 = pd(rt)
      val pd2 = pd(pd1)
      val mat1 = mt(pd2)
      floatingInputChannel(pd1).mappers === Seq(pd1, pd2)
    }

    e4 := {
      // pd1 floating -> pd2 floating -> mat
      val pd1 = pd(rt)
      val pd2 = pdWithEnv(pd1, mt(gbk(load)))
      val mat1 = mt(pd2)
      floatingInputChannel(pd1).mappers === Seq(pd1)
    }
  }

  trait MockInputChannel extends MscrInputChannel {
    tks = Map[Int, TaggedKey]().withDefaultValue(new MetadataTaggedKey { def metadataPath = "" }: TaggedKey)
    tvs = Map[Int, TaggedValue]().withDefaultValue(new MetadataTaggedValue { def metadataPath = ""  }: TaggedValue)
    environments = Map[ParallelDo, Any]().withDefaultValue(())
    emitters = Map[Int, EmitterWriter]().withDefaultValue(createEmitter(0, context))

    lazy val context = new InputOutputContext(null) {
      var key: Any = _
      var value: Any = _
      var tag: Int = _
      val keys = new ListBuffer[Any]
      val values = new ListBuffer[Any]

      override def write(k: Any, v: Any) {
        key = k match { case tk: TaggedKey => tk.get(tk.tag) }
        keys.append(key)
        value = v match { case tv: TaggedValue => tv.get(tv.tag) }
        tag = v match { case tv: TaggedValue => tv.tag }
        values.append(value)
      }
      override def configuration = new Configuration
    }

    override def scoobiConfiguration(configuration: Configuration) = ScoobiConfigurationImpl.unitEnv(configuration)
  }
  def floatingInputChannel(sourceNode: CompNode) = new FloatingInputChannel(sourceNode, new MscrsDefinition{}.floatingMappers(sourceNode)) with MockInputChannel
  def gbkInputChannel(sourceNode: CompNode, groupByKeys: Seq[GroupByKey] = Seq()) = new GbkInputChannel(sourceNode, groupByKeys) with MockInputChannel

  def beDistinct[T]: Matcher[Seq[T]] = (seq: Seq[T]) => (seq.distinct.size == seq.size, "The sequence contains duplicated elements:\n"+seq)
}
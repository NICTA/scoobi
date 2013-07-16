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

import testing.script.UnitSpecification
import org.specs2.specification.Groups
import comp.{Root, ParallelDo, GroupByKey, factory}
import org.specs2.matcher.{Matcher, ThrownExpectations}
import core.{EmitterWriter, MapFunction, InputOutputContext, CompNode}
import org.apache.hadoop.conf.Configuration
import rtt.{MetadataTaggedValue, MetadataTaggedKey, TaggedValue, TaggedKey}
import scala.collection.mutable.ListBuffer

class InputChannelSpec extends UnitSpecification with Groups with ThrownExpectations { def is = sequential ^ s2"""

 An InputChannel encapsulates parallelDo nodes which have a common source node.
 There are 2 types of InputChannels:

  * GbkInputChannel: which connects to at least one GroupByKey node
  * FloatingInputChannel: which connects to no GroupByKey (but to a Materialise node for example)
                                                                                                                       
Input-outputs
=============

 + input channels are identified by their unique id
 + the source of a GbkInput channel is the Load source if the sourceNode is a Load
 + the source of a GbkInput channel is the BridgeStore source if the sourceNode is a ProcessNode
 + the input nodes of the input channel are: the source node+the environments of all the channel mappers                 
 + an input channel defines tags which are all the gbks ids it relates to

 keys types
 ==========
 an input channel defines the types of keys emitted by tag
   + as the types of the gbks key for a GbkInputChannel                                                                  
   + as Int for a FloatingInputChannel

 values types
 ============
 an input channel defines the types of values emitted by tag
   + as the types of the gbks values for a GbkInputChannel                                                               
   + as the types of the last mappers for a FloatingInputChannel                                                         

Mappers
=======

 A gbk input channel has mappers defined by the parallelDos connected to its source
   + load -> pd1 -> gbk1 -> pd2 -> gbk2                                                                                  
   + load -> pd1 -> gbk1; pd1 -> gbk2                                                                                    
   + load -> pd1 -> gbk1 -> pd2 -> gbk2; pd1 -> gbk2                                                                     

 A floating input channel has mappers defined by the parallelDos connected to its source
   + load -> pd1 floating -> pd2 -> mat                                                                                  


Processing
==========

 An input channel must map key/values based on the mappers connected to its source
   + if there is only one mapper                                                                                         
   + if there are 2 independent mappers                                                                                  
   + if there are 2 consecutive mappers                                                                                  
   + if there are 3 mappers as a tree                                                                                    

"""

  "input-ouput" - new group with factory {
    val (l1, l2) = (load, load)
    val pd1 = pd(load)
    val (pd2, gbk1) = (pd(pd1), gbk(pd1))
    val gbk2 = gbk(pd2)

    eg := {
      val inputChannel = gbkInputChannel(pd1)
      inputChannel === inputChannel
      gbkInputChannel(pd1) !=== gbkInputChannel(pd1)
    }
    eg := { gbkInputChannel(l1).source === l1.source }
    eg := { gbkInputChannel(gbk1).source === gbk1.bridgeStore.get }
    eg := { gbkInputChannel(l1, Seq(gbk1, gbk2)).inputNodes === Seq(l1, pd1.env, pd2.env) }
    eg := { gbkInputChannel(l1, Seq(gbk1, gbk2)).tags === Seq(gbk1.id, gbk2.id) }
  }

  "input channels keys types" - new group with factory {
    eg := {
      val l1 = load; val pd1 = pd(l1)
      val (pd2, gbk1) = (pd(pd1), gbk(pd1)); val gbk2 = gbk(pd2)

      gbkInputChannel(l1, Seq(gbk1, gbk2)).keyTypes.tags === Seq(gbk1.id, gbk2.id)
      gbkInputChannel(l1, Seq(gbk1, gbk2)).keyTypes.types.values.toList === Seq((gbk1.wfk, gbk1.gpk), (gbk2.wfk, gbk2.gpk))
    }
    eg := {
      val l1 = load; val pd1 = pd(l1)
      val pd2 = pd(pd1); val mat1 = mt(pd2)

      floatingInputChannel(pd2, l1).keyTypes.tags === Seq(pd2.id)
      floatingInputChannel(pd2, l1).keyTypes.types.values.toList.mkString(",") === "(Int,AllGrouping)"
    }
  }
  "input channels values types" - new group with factory {
    eg := {
      val l1 = load; val pd1 = pd(l1)
      val (pd2, gbk1) = (pd(pd1), gbk(pd1)); val gbk2 = gbk(pd2)

      gbkInputChannel(l1, Seq(gbk1, gbk2)).valueTypes.tags === Seq(gbk1.id, gbk2.id)
      gbkInputChannel(l1, Seq(gbk1, gbk2)).valueTypes.types.values.toList === Seq(Tuple1(gbk1.wfv), Tuple1(gbk2.wfv))
    }
    eg := {
      val l1 = load; val pd1 = pd(l1)
      val pd2 = pd(pd1); val mat1 = mt(pd2)

      floatingInputChannel(pd2, l1).valueTypes.tags === Seq(pd2.id)
      floatingInputChannel(pd2, l1).valueTypes.types.values.toList === Seq(Tuple1(pd2.wf))
    }
  }

  "mappers" - new group with factory {
    eg := {
      // load -> pd1 -> gbk1 -> pd2 -> gbk2
      val l1 = load; val pd1 = pd(l1)
      val gbk1 = gbk(pd1); val pd2 = pd(gbk1); val gbk2 = gbk(pd2)
      gbkInputChannel(l1, Seq(gbk1)).mappers === Seq(pd1)
    }
    eg := {
      // load -> pd1 -> gbk1 -> pd2 -> gbk2
      //          |  -> gbk2
      val l1 = load; val pd1 = pd(l1)
      val (gbk1, gbk2) = (gbk(pd1), gbk(pd1)); val pd2 = pd(gbk1)
      gbkInputChannel(l1, Seq(gbk1, gbk2)).mappers === Seq(pd1)
    }
    eg := {
      // load -> pd1 -> gbk1 -> pd2
      //          |          -> pd2 -> gbk2
      val l1 = load; val pd1 = pd(l1)
      val gbk1 = gbk(pd1); val pd2 = pd(gbk1, pd1); val gbk2 = gbk(pd2)
      gbkInputChannel(l1, Seq(gbk1)).mappers === Seq(pd1)
    }
    eg := {
      // load -> pd1 -> pd2 -> mat
      val l1 = load; val pd1 = pd(l1)
      val pd2 = pd(pd1); val mat1 = mt(pd2)
      floatingInputChannel(pd2, l1).mappers === Seq(pd2, pd1)
    }
  }

  "mapping values" - new group with factory {
    val (l1, l2) = (load, load)
    val pd1 = pd(l1).copy(dofn = MapFunction(_.toString.toUpperCase))

    eg := {
      mt(pd1)
      val channel = floatingInputChannel(pd1, l1)
      channel.map("1", "start", channel.context)
      channel.context.key === 1
      channel.context.value === "START"
    }

    eg := {
      val pd2 = pd(l1).copy(dofn = MapFunction(_.toString.toLowerCase))
      val (mt1, mt2) = (mt(pd1), mt(pd2))
      aRoot(mt1, mt2)

      val channel = floatingInputChannel(Seq(pd1, pd2), l1)
      channel.map("1", "stARt", channel.context)
      channel.context.keys must beDistinct
      channel.context.values === Seq("START", "start")
    }

    eg := {
      val pd2 = pd(pd1).copy(dofn = MapFunction(_.toString+" now"))
      mt(pd2)

      val channel = floatingInputChannel(pd2, l1)
      channel.map("1", "stARt", channel.context)
      channel.context.values === Seq("START now")
    }

    eg := {
      val pd2 = pd(pd1).copy(dofn = MapFunction(a => (a, a.toString+" now")))
      val pd3 = pd(pd1).copy(dofn = MapFunction(a => (a, a.toString+" later")))
      val (gbk1, gbk2) = (gbk(pd2), gbk(pd3))
      aRoot(gbk1, gbk2)

      val channel = gbkInputChannel(l1, Seq(gbk1, gbk2))
      channel.map("1", "stARt", channel.context)
      channel.context.values === Seq("START now", "START later")
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
  def floatingInputChannel(terminalNodes: Seq[CompNode], sourceNode: CompNode): FloatingInputChannel with MockInputChannel = {
    val graph = new MscrsDefinition {}
    graph.reinit(Root(terminalNodes))
    new FloatingInputChannel(sourceNode, terminalNodes, graph) with MockInputChannel
  }

  def floatingInputChannel(terminalNode: CompNode, sourceNode: CompNode): FloatingInputChannel with MockInputChannel =
    floatingInputChannel(Seq(terminalNode), sourceNode)

  def gbkInputChannel(sourceNode: CompNode, groupByKeys: Seq[GroupByKey] = Seq()) = {
    val graph = new MscrsDefinition {}
    graph.reinit(Root(groupByKeys))
    new GbkInputChannel(sourceNode, groupByKeys, graph) with MockInputChannel
  }

  def beDistinct[T]: Matcher[Seq[T]] = (seq: Seq[T]) => (seq.distinct.size == seq.size, "The sequence contains duplicated elements:\n"+seq)
}
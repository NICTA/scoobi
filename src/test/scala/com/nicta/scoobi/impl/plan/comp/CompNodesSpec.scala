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
package comp

import testing.mutable.UnitSpecification
import org.specs2.specification.AllExpectations
import core._
import core.WireFormat._
import org.apache.hadoop.conf._
import ParallelDo._
import mapreducer.VectorEmitterWriter
import com.nicta.scoobi.io.text.TextOutput._

class CompNodesSpec extends UnitSpecification with AllExpectations with CompNodeData {

  "CompNodes equality is based on a unique node id" >> prop { (node1: CompNode, node2: CompNode) =>
    node1 must be_==(node2).iff(node1.id == node2.id)
  }

  "CompNodes hashCode is based on a unique node id" >> prop { (node1: CompNode, node2: CompNode) =>
    node1.hashCode must be_==(node2.hashCode).when(node1.id == node2.id)
  }

  "the inputs of a node are its children" >> new nodes {
    val load0 = load
    val cb1 = cb(load0)
    (cb1 -> inputs) ==== Seq(load0)
  }
  "the inputs of a parallelDo only contain the input node, not the environment" >> new nodes {
    val ld1 = load
    (pd(ld1) -> inputs) ==== Seq(ld1)
  }; endp

  "it is possible to collect all the sinks of a computation graph" >> new factory {
    val (s1, s2, s3) = (textFileSink("s1"), textFileSink("s2"), textFileSink("s3"))
    val l1 = load
    val (pd1, pd2) = (pd(l1).addSink(s1), pd(l1).addSink(s2))
    val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
    val mt1 = mt(pd(gbk1, gbk2).addSink(s3))

    allSinks(mt1).toSet === Set(s1, s2, s3, gbk1.bridgeStore.get ,gbk2.bridgeStore.get)
  }

  "it is possible to get all the nodes which use a given node as an environment" >> new factory {
    val mt1 = mt(pd(load))
    val pd1 = pdWithEnv(load, mt1)

    (mt1 -> usesAsEnvironment) === Seq(pd1)
  }; endp

  "2 ParallelDo nodes can be fused together" >> {
    val pd1 = ParallelDo.create(loadWith("hello"))(wireFormat[String])

    "simple case: 2 pass-through ParallelDos, the input value must be unchanged" >> new factory {
      pdMap(fuse(pd1, ParallelDo.create(pd1)(wireFormat[String])), "world") === Seq("world")
    }
    "1 pass-through ParallelDo + a toUpper map" >> new factory {
      pdMap(fuse(pd1, pdFromFunction(pd1, (_:String).toUpperCase)), "world") === Seq("WORLD")
    }
    "2 consecutive maps" >> new factory {
      pdMap(fuse(pdFromFunction(load, (_:String) + " world"),
                 pdFromFunction(pd1, (_:String).toUpperCase)), "hello") === Seq("HELLO WORLD")
    }
  }

  def pdFromFunction(in: CompNode, f: String => String) = {
    val dofn = BasicDoFunction((env: Any, input: Any, emitter: EmitterWriter) => emitter.write(f(input.toString)))
    ParallelDo(Seq(load), rt, dofn, wireFormat[String], wireFormat[String])
  }

  def pdMap(pd: ParallelDo, value: Any): Any = {
    val emitter = VectorEmitterWriter()
    pd.map(("",""), value, emitter)
    emitter.result
  }
  /** this configuration doesn't store any value in the environment */
  def configuration = ScoobiConfigurationImpl.unitEnv(new Configuration)
}

trait nodes extends factory {
  lazy val l1   = load
  lazy val pd1  = pd(l1)
  lazy val gbk1 = gbk(pd1)
  lazy val pds1 = pd(gbk1, pd1)
  lazy val mat1 = mt(pds1)
}
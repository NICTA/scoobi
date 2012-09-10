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

import application.ScoobiConfiguration
import plan._
import plan.BypassInputChannel
import plan.AST.Load
import plan.MapperInputChannel
import org.specs2.mock.Mockito
import testing.CommandLineHadoopLogFactory
import testing.mutable.UnitSpecification

class ExecutorSpec extends UnitSpecification with Mockito with CommandLineHadoopLogFactory {

  // sample graph for the examples, with 2 Mscrs and 3 BridgeStores
  lazy val bs1 :: bs2 :: bs3 :: _ = Seq.fill(3)(mockBridgeStore)
  lazy val (mscr1, mscr2) = (MSCR(Set(bypassInput(bs1)), Set()), MSCR(Set(mapperInput(bs2)), Set()))
  lazy val graph1 = new MSCRGraph(Nil, Set(mscr1, mscr2), Map(Load[String]() -> bs3), Map())
  lazy val state1 = Executor.prepare(graph1, ScoobiConfiguration())

  "The Executor prepares the execution by counting the number of references on BridgeStores" >> {
    state1.refcnts.keys must have size(3)
    state1.refcnts.values must containTheSameElementsAs(Seq(1, 1, 1))
  }
  "During the execution, when the number of references on a BridgeStore reaches 0, the intermediate data is removed" >> {
    val state = Executor.freeIntermediateOutputs(mscr1, state1)
    state.refcnts.map { case (bs, size) => (bs.id, size) } must contain("1" -> 0, "2" -> 1, "3" -> 1)
    there was one(bs1).freePath
  }

  def bypassInput(bs: BridgeStore[_]) =
    new BypassInputChannel(bs, null)

  def mapperInput(bs: BridgeStore[_]) =
    new MapperInputChannel(bs, Set()) {
      def inputNode: AST.Node[_, _ <: Shape] = null
      def inputEnvs: Set[AST.Node[_, _ <: Shape]] = Set()
      def nodes: Set[AST.Node[_, _ <: Shape]] = Set()
    }

  lazy val ids = Stream.from(1).map(_.toString).iterator
  def mockBridgeStore: BridgeStore[Int] = {
    val bs = mock[BridgeStore[Int]]
    bs.id returns ids.next
    bs
  }
}

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

import org.scalacheck.{Gen, Arbitrary}
import core._
import testing.mutable.UnitSpecification
import org.specs2.mutable.Tables
import com.nicta.scoobi.io.text.TextOutput
import TextOutput._
import control.Functions._

class OptimiserSpec extends UnitSpecification with Tables with CompNodeData {

  "A Combine which doesn't have a GroupByKey as an Input must be transformed to a ParallelDo" >> new optimiser {
    "input"                                        | "expected"                                 |>
     cb(load)                                      ! pd(load)                                   |
     cb(pd(load))                                  ! pd(pd(load))                               |
     cb(gbk(load))                                 ! cb(gbk(load))                              | { (input, output) =>
       showStructure(optimise(combineToParDo, input).head) ==== showStructure(output)
     }
  }

  "Successive ParallelDos must be fused" >> prop { (node: CompNode, f: factory) => import f._
    val optimised = optimise(parDoFuse, node).head
    collectSuccessiveParDos(optimised) must beEmpty
  }

  "If the input node of a Materialise node has no bridgeStore we need to create one" >> new optimiser {
    val materialise = mt(pd(load).addSink(textFileSink("path")))
    materialise.in.bridgeStore must beNone
    optimise(addBridgeStore, materialise).collect(isAMaterialise).head.in.bridgeStore must beSome
  }

  "If some of the sinks of a node have not been filled, a new node must be created for it" >> new optimiser {
    val list = pd(load).addSink(markSinkAsFilled(textFileSink("path")))

    val sinks = optimise(addParallelDoForNonFilledSinks, list.addSink(textFileSink("path"))).collect(isAParallelDo).head.nodeSinks
    sinks must haveSize(1)
    forall(sinks) { sink => hasBeenFilled(sink) must beFalse }
  }

  trait optimiser extends factory with Optimiser

  implicit def arbitraryFactory: Arbitrary[factory] = Arbitrary(Gen.value(new factory{}))
}


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
import CollectFunctions._

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

  "Shared ParallelDos must not duplicated" >> {
    "when no fusion occurs" >> new optimiser {
      val pd1 = pd(load)
      val (gbk1, gbk2) = (gbk(pd1), gbk(pd1))
      val node = aRoot(gbk1, gbk2)
      val optimised = optimise(parDoFuse, node)
      collectParallelDo(optimised.head).distinct must haveSize(1)
    }
    "with fusion, the nodes must be fused but only if they are not shared" >> new optimiser {
      val pd1 = pd(load)
      val (pd2, pd3) = (pd(pd1), pd(pd1))
      val (gbk1, gbk2) = (gbk(pd(pd2)), gbk(pd(pd2)))
      val node = aRoot(gbk1, gbk2)
      val optimised = optimise(parDoFuse, node)

      collectParallelDo(optimised.head).distinct must haveSize(3)
    }
  }

  "Shared Combine nodes must not duplicated" >> {
    "with cd -> pd transformation" >> new optimiser {
      val cb1 = cb(load)
      val (gbk1, gbk2) = (gbk(cb1), gbk(cb1))
      val node = aRoot(gbk1, gbk2)
      val optimised = optimise(combineToParDo, node)
      collectParallelDo(optimised.head).distinct must haveSize(1)
    }
    "with cb -> pd and node fusion" >> new optimiser {
      val pd1 = pd(load)
      val pd2 = pd(pd1)
      val cb1 = cb(pd2)
      val (gbk1, gbk2) = (gbk(cb1), gbk(cb1))
      val node = aRoot(gbk1, gbk2)
      val optimised = optimise(combineToParDo <* parDoFuse, node)
      collectParallelDo(optimised.head).distinct aka pretty(optimised.head) must haveSize(1)
    }
  }

  "If some of the sinks of a node have not been filled, a new node must be created for it" >> new optimiser {
    val sink = textFileSink("path")
    val list = pd(load).addSink(sink)
    list.sinks.foreach(markSinkAsFilled)

    val sinks = optimise(addParallelDoForNonFilledSinks, list.addSink(textFileSink("path"))).collect(isAParallelDo).head.nodeSinks
    sinks must haveSize(1)
    forall(sinks) { sink => hasBeenFilled(sink) must beFalse }
  }

  "It must be possible to optimise a parallelDo with lots of inputs, with no SOE" >> new optimiser {
    optimise(pd((1 to 10000).map(_ => pd(load)):_*))
    ok
  }

  trait optimiser extends factory with Optimiser

  implicit def arbitraryFactory: Arbitrary[factory] = Arbitrary(Gen.const(new factory{}))
}


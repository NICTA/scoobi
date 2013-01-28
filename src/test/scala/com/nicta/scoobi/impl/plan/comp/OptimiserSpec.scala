package com.nicta.scoobi
package impl
package plan
package comp

import org.scalacheck.{Gen, Arbitrary, Prop}
import org.kiama.rewriting._
import core._
import testing.mutable.UnitSpecification
import org.specs2.mutable.Tables

class OptimiserSpec extends UnitSpecification with Tables with CompNodeData {

  "A Combine which doesn't have a GroupByKey as an Input must be transformed to a ParallelDo" >> new factory with Optimiser {
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

  implicit def arbitraryFactory: Arbitrary[factory] = Arbitrary(Gen.value(new factory{}))
}


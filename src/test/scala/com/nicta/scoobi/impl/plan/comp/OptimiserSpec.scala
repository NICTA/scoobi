package com.nicta.scoobi
package impl
package plan
package comp

import org.scalacheck.{Gen, Arbitrary, Prop}
import org.kiama.rewriting._
import core._
import testing.mutable.UnitSpecification
import org.specs2.mutable.Tables
import com.nicta.scoobi.io.text.TextOutput

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
    val materialise = mt(pd(load).addSink(TextOutput.textFileSink("path")))
    materialise.in.bridgeStore must beNone
    optimise(addBridgeStore, materialise).collect(isAMaterialise).head.in.bridgeStore must beSome
  }

  trait optimiser extends factory with Optimiser

  implicit def arbitraryFactory: Arbitrary[factory] = Arbitrary(Gen.value(new factory{}))
}


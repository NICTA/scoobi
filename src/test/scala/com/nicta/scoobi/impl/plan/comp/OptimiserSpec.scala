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

  "1. Combines must be optimised" >> {
    "A Combine which doesn't have a GroupByKey as an Input must be transformed to a ParallelDo" >> new nodes {
      "input"                                        | "expected"                                    |>
       cb(l1)                                        ! pd(l1)                                        |
       cb(pd(l1))                                    ! pd(pd(l1))                                    |
       cb(gbk(l1))                                   ! cb(gbk(l1))                                   | { (input, output) =>
         showStructure(optimise(combineToParDo, input).head) ==== showStructure(output)
       }
    }
    "Any optimised Combine in the graph can only have GroupByKey as an input" >> prop { (node: CompNode, f: factory) => {}; import f._
      forall(collectCombine(optimise(combineToParDo, node).head)) { n =>
        n must beLike { case Combine1(GroupByKey1(_)) => ok }
      }
    }
    "After optimisation, all the transformed Combines must be ParallelDo" >> prop { (node: CompNode, f: factory) => import f._
      val optimised = optimise(combineToParDo, node).head
      (collectCombine(node).size + collectParallelDo(node).size) ===
      (collectCombineGbk(optimised).size + collectParallelDo(optimised).size)
    }
  }

  "2. Successive ParallelDos must be fused" >> prop { (node: CompNode, f: factory) => import f._
    val optimised = optimise(parDoFuse(pass = 1), node).head
    collectSuccessiveParDos(optimised) must beEmpty
  };p

  trait nodes extends factory with Optimiser with CompNodes {
    lazy val (l1, l2)   = (load, load)
    lazy val pd1        = pd(l1)
    lazy val pds        = pd(l1, l2)
    lazy val gbk1       = gbk(l1)
    lazy val mt1        = mt(gbk1)
    lazy val gbkf1      = gbk(pds)
    lazy val combine1   = cb(l1)
    lazy val combine2   = cb(l2)

    def nodesAreDistinct(nodes: CompNode*) = nodes.distinct.size aka nodes.mkString(", ") must_== nodes.size

  }


  implicit def arbitraryFactory: Arbitrary[factory] = Arbitrary(Gen.value(new factory{}))
}


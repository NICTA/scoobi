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

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import org.specs2._
import matcher.ThrownExpectations
import specification.Groups
import testing.UnitSpecification
import plan._
import comp._
import core._
import mscr._

class MscrsDefinitionSpec extends UnitSpecification with Groups with ThrownExpectations { def is =

  "The gbks of a graph can be sorted in layers according to their dependencies"                                         ^
    "all the nodes in a layer cannot be parent of each other"                                                           ! g1().e1^
    "2 different layers have at least 2 nodes parent of each other"                                                     ! g1().e2^
                                                                                                                        endp^
  "For each layer in the topological sort, we can create Mscrs"                                                         ^
    "Output channels"                                                                                                   ^ section("outputs")^
      "each gbk belongs to a GbkOutputChannel"                                                                          ! g2().e1^
      "aggregating the combine node if there is one after the gbk"                                                      ! g2().e2^
      "aggregating the pd node if there is one after the gbk"                                                           ! g2().e3^
      "aggregating the combine and pd nodes if they are after the gbk"                                                  ! g2().e4^
                                                                                                                        p^ section("outputs")^
    "Input channels"                                                                                                    ^
      "all mappers sharing the same input go to the same MscrInputChannel"                                              ! g3().e1^
      "a mapper is defined as an 'inside' mapper if it is used by a non-floating mapper"                                ! g3().e2^
                                                                                                                        p^
    "Mscr creation"                                                                                                     ^ section("creation")^
      "there must be one mscr per set of related tags"                                                                  ! g4().e1^
                                                                                                                        end


  "layering of Gbk layers" - new g1 with definition {
    import scalaz.Scalaz._

    e1 := prop { layer: Layer[CompNode] =>
      val nodes = layer.nodes
      nodes.forall(n => !nodes.exists(_ -> isStrictParentOf(n)))
    }.set(minTestsOk -> 100)

    e2 := forAll(genLayerPair) { (pair: (Layer[CompNode], Layer[CompNode])) => val (layer1, layer2) = pair
      val pairs = ^(layer1.gbks.toStream, layer2.gbks.toStream)((_,_))

      forallWhen(pairs) { case (n1, n2) =>
        (n1 -> isStrictParentOf(n2)) aka (showGraph(n1)+"\n"+showGraph(n2)) must beTrue
      }

    }.set(minTestsOk -> 100, maxSize -> 6, maxDiscarded -> 150)

  }

  "Output channels" - new g2 with definition {

    e1 := {
      val gbk1 = gbk(load)
      gbkOutputChannel(gbk1) === GbkOutputChannel(gbk1)
    }
    e2 := {
      val gbk1 = gbk(load)
      val cb1 = cb(gbk1)
      gbkOutputChannel(gbk1) === GbkOutputChannel(gbk1, combiner = Some(cb1))
    }
    e3 := {
      val gbk1 = gbk(load)
      val pd1 = pd(gbk1)
      gbkOutputChannel(gbk1) === GbkOutputChannel(gbk1, reducer = Some(pd1))
    }
    e4 := {
      val gbk1 = gbk(load)
      val cb1 = cb(gbk1)
      val pd1 = pd(cb1)
      gbkOutputChannel(gbk1) === GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(pd1))
    }
  }

  "Input channels" - new g3 with definition {
    e1 := {
      val l1 = load
      val (pd1, pd2, pd3) = (pd(l1), pd(l1), pd(l1))
      val (gbk1, gbk2, gbk3) = (gbk(pd1), gbk(pd2), gbk(pd3))
      val layer1    = layers(aRoot(gbk1, gbk2, gbk3)).head

      gbkInputChannels(layer1) must  have size(1)
    }

    e2 := {
      "pd(load) is not an inside mapper" <==> { isInsideMapper(pd(load)) ==== false }

      val pd1 = pd(load)
      gbk(pd(pd1))
      "pd1 is an inside mapper" <==> { isInsideMapper(pd1) ==== true }
    }
  }

  "Mscr creation" - new g4 with definition {

    e1 := {
      /**
       *             Mscr1             Mscr2
       *
       *         l1            l2       l3
       *        /   \        /    \      |
       *     pd1     pd2  pd3     pd4   pd5
       *       \         X        /      |
       *       flatten1     flatten2     |
       *           |           |         |
       *           gbk1       gbk2      gbk3
       */
      val (l1, l2, l3) = (load, load, load)
      val (pd1, pd2, pd3, pd4, pd5) = (pd(l1), pd(l1), pd(l2), pd(l2), pd(l3))
      val (flatten1, flatten2) = (pd(pd1, pd3), pd(pd2, pd4))
      val (gbk1, gbk2, gbk3) = (gbk(flatten1), gbk(flatten2), gbk(pd5))
      val layer1    = layers(aRoot(gbk1, gbk2, gbk3)).head

      val layer1Mscrs = mscrs(layer1)
      layer1Mscrs must have size(2)

      val (mscr1, mscr2) = (layer1Mscrs(0), layer1Mscrs(1))
      mscr1.sources.size === 2
      mscr1.bridges.size === 2

      mscr2.sources.size === 1
      mscr2.bridges.size === 1
    }
  }

}
trait definition extends factory with CompNodeData {
  implicit val arbitraryLayer: Arbitrary[Layer[CompNode]] = Arbitrary(genLayer)

  // make sure there is at least one layer
  // by construction, there is no cycle
  val genLayers = arbitraryCompNode.arbitrary.map { n =>
    resetMemo()             // reset the memos otherwise too much data accumulates during testing!
    layers(gbk(pd(gbk(n)))) // generate at least 2 layers
  }
  val genLayer     = genLayers.flatMap(ls => Gen.pick(1, ls)).map(_.head)
  val genLayerPair = genLayers.flatMap(ls => Gen.pick(2, ls)).map(ls => (ls(0), ls(1))).filter { case (l1, l2) => l1 != l2 }

  def graph(layer: Layer[CompNode]) =
    if (layer.nodes.isEmpty) "empty layer - shouldn't happen"
    else                     layer.nodes.map(showGraph).mkString("\nshowing graphs for layer\n", "\n", "\n")

  override def delayedInit(body: => Unit) { body }
}

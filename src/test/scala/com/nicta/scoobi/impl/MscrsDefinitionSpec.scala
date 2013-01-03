package com.nicta.scoobi
package impl

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import org.specs2._
import plan.mscr._
import plan.mscr.GbkOutputChannel
import specification.Groups
import matcher.ThrownExpectations
import plan.comp._
import core._
import testing.UnitSpecification

class MscrsDefinitionSpec extends UnitSpecification with Groups with ThrownExpectations with CompNodeData { def is =

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
      "there is a bypass output channel for each mapper having outputs other than a gbk"                                ! g2().e5^
                                                                                                                        endp^ section("outputs")^
    "Input channels"                                                                                                    ^
      "GbkOutputChannels have inputs, some of them are Mappers"                                                         ^
      "all mappers sharing the same input go to the same MapperInputChannel"                                            ! g3().e1^
      "other mappers go to an individual MapperInputChannel"                                                            ! g3().e2^
      "other Gbk inputs go to an IdInputChannel"                                                                        ! g3().e3^
                                                                                                                        endp^
    "Mscr creation"                                                                                                     ^ section("creation")^
      "output channels must have a unique tag"                                                                          ! g4().e1^
      "the set of tags of an input channel must be all the tags of its output channels"                                 ! g4().e2^
      "there must be one mscr per set of related tags"                                                                  ! g4().e3^
      "the mscrs are all the gbk mscrs on the layer + the mscrs for parallel do nodes which are not in a gbk mscr"      ! g4().e4^
                                                                                                                        end

/*
  "layering of Gbk layers" - new g1 with definition { import scalaz.Scalaz._

    e1 := prop { layer: Layer[CompNode] =>
      val nodes = layer.nodes

      nodes must not(beEmpty)
      nodes.forall(n => !nodes.exists(_ -> isStrictParentOf(n)))
    }.set(minTestsOk -> 100)

    e2 := forAll(genLayerPair) { (pair: (Layer[CompNode], Layer[CompNode])) => val (layer1, layer2) = pair
      val pairs = ^(layer1.gbks.toStream, layer2.gbks.toStream)((_,_))
      val parentChild = pairs.find { case (n1, n2) => !(n1 -> isStrictParentOf(n2)) }
      lazy val showParentChild = parentChild.collect { case (n1, n2) => (showGraph(n1)+"\n"+showGraph(n2)) }.getOrElse("")

      parentChild aka showParentChild must beNone

    }.set(minTestsOk -> 100, maxSize -> 6, maxDiscarded -> 150)

  }

  "Output channels" - new g2 with definition {

    e1 := {
      val gbk1 = gbk(load)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1)
    }
    e2 := {
      val gbk1 = gbk(load)
      val cb1 = cb(gbk1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, combiner = Some(cb1))
    }
    e3 := {
      val gbk1 = gbk(load)
      val pd1 = pd(gbk1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, reducer = Some(pd1))
    }
    e4 := {
      val gbk1 = gbk(load)
      val cb1 = cb(gbk1)
      val pd1 = pd(cb1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(pd1))
    }
  }

  "Input channels" - new g3 with definition with simpleGraph {

    e1 := {
      val graph = pd(gbk1, gbk2)
      val ls    = layers(graph)
      val inputChannels: Seq[MapperInputChannel] = mapperInputChannels(ls.head).toSeq

      inputChannels must have size(1)
    }

    e2 := {
      val graph = pd(gbk1, gbk2, gbk3)
      val ls    = layers(graph)
      val inputChannels: Seq[MapperInputChannel] = mapperInputChannels(ls.head).toSeq.reverse

      inputChannels must have size(2)
    }

    e3 := {
      val graph = pd(gbk1, gbk2, gbk3, gbk4)
      val ls    = layers(graph)
      val channels: Seq[InputChannel] = gbkInputChannels(ls.head).toSeq

      channels must have size(3)
    }
  }

  "Mscr creation" - new g4 with definition with simpleGraph {
    lazy val graph  = pd(gbk1, gbk2, gbk3, gbk4)
    lazy val layer1 = layers(graph).head

    e1 := {
      val tags = gbkOutputChannels(layer1).map(_.tag)
      "there are as many tags as output channels on a layer" ==> {
        tags.toSet must have size(layer1.gbks.size)
      }
    }
    e3 := {
      mscrs(layers(graph).head) must have size(3)
    }
    e4 := {
      val graph2 = pd(gbk1, gbk2, gbk3, gbk4, mt(pd(load)))
      val layer1 = layers(graph2).head

      layers(graph2) must have size(1)
      "there are 5 mscrs in total" ==> { mscrs(layer1) must have size(4) }
      "there is one pd mscr"       ==> { pdMscrs(layer1) must have size(1) }
      "there are 3 gbk mscrs"      ==> { gbkMscrs(layer1) must have size(3) }
    }
  }

  trait simpleGraph extends nodesFactory {
    val ld1 = load
    val (pd1, pd2, cb1) = (pd(ld1), pd(ld1), cb(load))
    val (gbk1, gbk2, gbk3, gbk4) = (gbk(pd1), gbk(pd2), gbk(pd(load)), gbk(cb1))
  }

  trait definition extends nodesFactory with MscrsDefinition {
    implicit val arbitraryLayer: Arbitrary[Layer[CompNode]] = Arbitrary(genLayer)

    // make sure there is at least one layer
    // by construction, there is no cycle
    val genLayers = arbitraryCompNode.arbitrary.map { n =>
      resetMemo()             // reset the memos otherwise too much data accumulates during testing!
      layers(gbk(pd(gbk(n)))) // generate at least 2 layers
    }
    val genLayer     = genLayers.flatMap(ls => Gen.pick(1, ls)).map(_.head)
    val genLayerPair = genLayers.flatMap(ls => Gen.pick(2, ls)).map { ls => (ls(0), ls(1)) }

    def graph(layer: Layer[CompNode]) =
      if (layer.nodes.isEmpty) "empty layer - shouldn't happen"
      else                     layer.nodes.map(showGraph).mkString("\nshowing graphs for layer\n", "\n", "\n")
  }
*/
}


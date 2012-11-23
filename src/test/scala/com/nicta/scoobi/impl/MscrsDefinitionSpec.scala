package com.nicta.scoobi
package impl

import org.kiama.attribution.Attribution
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import org.specs2._
import org.specs2.collection.Iterablex._
import plan.comp.GroupByKey
import plan.mscr.{IdInputChannel, InputChannel, MapperInputChannel, GbkOutputChannel}
import specification.Groups
import matcher.ThrownExpectations
import plan.comp._
import core.{DList, CompNode}
import testing.UnitSpecification
import collection.IdSet._
import application.DList
import application.DList

class MscrsDefinitionSpec extends UnitSpecification with Groups with ThrownExpectations with CompNodeData { def is =

  "The gbks of a graph can be sorted in a topological sort"                              ^
    "all the nodes in a layer cannot be parent of each other"                            ! g1().e1^
    "2 different layers have at least 2 nodes parent of each other"                      ! g1().e2^
                                                                                         endp^
  "For each layer in the topological sort, we can create Mscrs"                          ^
    "Output channels"                                                                    ^
      "each gbk belongs to a GbkOutputChannel"                                           ! g2().e1^
      "aggregating the flatten node if there is one before the gbk"                      ! g2().e2^
      "aggregating the combine node if there is one after the gbk"                       ! g2().e3^
      "aggregating the pd node if there is one after the gbk"                            ! g2().e4^
      "aggregating the combine and pd nodes if they are after the gbk"                   ! g2().e5^
                                                                                         endp^ section("inputs")^
    "Input channels"                                                                     ^
      "GbkOutputChannels have inputs, some of them are Mappers"                          ^
      "all mappers sharing the same input go to the same MapperInputChannel"             ! g3().e1^
      "other mappers go to an individual MapperInputChannel"                             ! g3().e2^
      "other Gbk inputs go to an IdInputChannel"                                         ! g3().e3^
                                                                                         endp^
    "Mscr creation"                                                                      ^
      "GbkOutputChannels sharing the same MapperInputChannels belong to the same Mscr"   ! g4().e4^
                                                                                         end


  "topological sort of Gbk layers" - new g1 with definition {

    e1 := prop { layer: Layer[GBK] =>
      val nodes = layer.nodes
      nodes.forall(n => !nodes.exists(_ -> isStrictParentOf(n))) ==== true
    }.set(minTestsOk -> 10)

    e2 := forAll(genLayerPair) { (pair: (Layer[GBK], Layer[GBK])) =>
      val (layer1, layer2) = pair
      (layer1 ==== layer2) or
      (layer1.nodes.exists(n1 => layer2.nodes.exists(_ -> isStrictParentOf(n1))) ==== true)
    }.set(minTestsOk -> 20)

  }

  "Gbk output channels" - new g2 with definition {
    e1 := {
      val gbk1 = gbk(load)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1)
    }
    e2 := {
      val fl1 = flatten(load)
      val gbk1 = gbk(fl1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, flatten = Some(fl1))
    }
    e3 := {
      val gbk1 = gbk(load)
      val cb1 = cb(gbk1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, combiner = Some(cb1))
    }
    e4 := {
      val gbk1 = gbk(load)
      val pd1 = pd(gbk1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, reducer = Some(pd1))
    }
    e5 := {
      val gbk1 = gbk(load)
      val cb1 = cb(gbk1)
      val pd1 = pd(cb1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(pd1))
    }
  }

  "Input channels" - new g3 with definition {
    e1 := {
      val ld1 = load
      val (pd1, pd2) = (pd(ld1), pd(ld1))
      val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
      val layer: Layer[T] = layers(gbk1).head
      val inputChannels: Seq[MapperInputChannel] = mapperInputChannels(layer).toSeq

      inputChannels must have size(1)
      inputChannels.head.parDos must have size(2)
    }
  }

  trait definition extends nodesFactory with MscrsDefinition {
    implicit val arbitraryLayer: Arbitrary[Layer[GBK]] = Arbitrary(genLayer)

    // make sure there is at least one layer
    // by construction, there is no cycle
    val genLayers = arbitraryCompNode.arbitrary.map { n =>
      resetMemo()             // reset the memos otherwise too much data accumulates during testing!
      layers(gbk(pd(gbk(n)))) // generate at least 2 layers
    }
    val genLayer     = genLayers.flatMap(ls => Gen.pick(1, ls)).map(_.head)
    val genLayerPair = genLayers.flatMap(ls => Gen.pick(2, ls)).map { ls => (ls(0), ls(1)) }

    def graph(layer: Layer[GBK]) =
      if (layer.nodes.isEmpty) "empty layer - shouldn't happen"
      else                     layer.nodes.map(showGraph).mkString("\nshowing graphs for layer\n", "\n", "\n")
  }

  trait MscrsDefinition extends CompNodes with TopologicalSort {
    type T = GBK

    def selectNode(n: CompNode) = !Seq(n).collect(isAGroupByKey).isEmpty

    lazy val gbkOutputChannel: GBK => GbkOutputChannel = attr { case g  =>
      val flatten = Seq(g.in).collect(isAFlatten).headOption

      (g -> ancestors).toList match {
        case (c @ Combine1(_)) :: (p @ ParallelDo1(_)) :: rest => GbkOutputChannel(g, flatten, combiner = Some(c), reducer = Some(p))
        case (c @ Combine1(_)) :: rest                         => GbkOutputChannel(g, flatten, combiner = Some(c))
        case (p @ ParallelDo1(_)) :: rest                      => GbkOutputChannel(g, flatten, reducer = Some(p))
        case _                                                 => GbkOutputChannel(g)
      }
    }

    lazy val mapperInputChannels: Layer[T] => Set[MapperInputChannel] = attr { case layer =>
      val mappers = layer.nodes.flatMap(_ -> inputs).flatMap {
        case Flatten1(ins)         => ins.collect(isAParallelDo)
        case pd: ParallelDo[_,_,_] => Seq(pd)
      }.filterNot(_ -> isReducer)

      mappers.groupBy(_.in.id).values.map(pds => MapperInputChannel(pds:_*)).toSet
    }

    lazy val idInputChannels: Layer[T] => Set[IdInputChannel] = attr { case layer =>
      Set()
    }

    lazy val isReducer: ParallelDo[_,_,_] => Boolean = attr { case pd =>
      (pd -> descendents).collect(isAGroupByKey).map(gbkOutputChannel).exists(_.reducer == Some(pd))
    }

  }
}

import scalaz.Scalaz._

trait TopologicalSort extends CompNodes with Attribution with ShowNode {

  type T <: CompNode

  /** a function to select only some nodes in the graph. They must be of type T */
  def selectNode(n: CompNode): Boolean

  lazy val selected: CompNode => Boolean = attr { case n => selectNode(n) }

  /**
   * the set of layers for a given node is defined by:
   *  all the layers above a target node +
   *  all the layers below a target node +
   *  the layer for the current target node if there is one
   */
  lazy val layers: CompNode => Set[Layer[T]] = circular(Set[Layer[T]]()) { case n =>
    (n -> inputsOutputs).flatMap(_ -> layers) ++ (n -> layerOption).toSet
  }

  /**
   * A layer is defined by all the nodes which have no parent-child relationship in the set of all nodes
   */
  lazy val layer: T => Layer[T] = attr {
    case n => Layer((n -> allNodes).toSeq.filterNot(_ -> isStrictParentOf(n)):_*)
  }
  /**
   * A layer is only defined on selected nodes
   */
  lazy val layerOption: CompNode => Option[Layer[T]] = attr {
    case n => (n -> selected).option(n.asInstanceOf[T] -> layer)
  }

  /**
   * all target nodes in the graph.
   */
  lazy val allNodes: CompNode => Set[T] = circular(Set[T]()) {
    case n =>
      (n -> selected).option(n.asInstanceOf[T]).toSet ++  // create a set for the current node if selected
      (n -> inputsOutputs).map(_ -> allNodes).flatten     // add to all other nodes
  }

  case class Layer[+T](nodes: T*)
}

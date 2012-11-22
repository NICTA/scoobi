package com.nicta.scoobi
package impl

import org.kiama.attribution.Attribution
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import org.specs2._
import org.specs2.collection.Iterablex._
import plan.comp.GroupByKey
import specification.Groups
import matcher.ThrownExpectations
import plan.comp._
import core.CompNode

class MscrsDefinitionSpec extends Specification with Groups with ThrownExpectations with CompNodeData { def is =

 "The gbks of a graph can be sorted in a topological sort" ^
   "all the nodes in a layer cannot be parent of each other"       ! g1().e1^
   "2 different layers have at least 2 nodes parent of each other" ! g1().e2^
   end

  "topological sort" - new g1 with definition {
    e1 := prop { layer: Layer[GBK] =>
      val nodes = layer.nodes
      nodes.forall(n => !nodes.exists(_ -> isStrictParentOf(n))) ==== true
    }
    e2 := forAll(genLayerPair) { (pair: (Layer[GBK], Layer[GBK])) =>
      val (layer1, layer2) = pair
      (layer1 ==== layer2) or
      (layer1.nodes.exists(n1 => layer2.nodes.exists(_ -> isStrictParentOf(n1))) ==== true)
    }.set(minTestsOk -> 30)
  }

  trait definition extends factory with MscrsDefinition with ShowNode {
    implicit val arbitraryLayer: Arbitrary[Layer[GBK]] = Arbitrary(genLayer)
    // make sure there is at least one layer
    val genLayers    = arbitraryCompNode.arbitrary.map(n => layers(gbk(n)))
    val genLayer     = genLayers.flatMap(ls => Gen.pick(1, ls)).map(_.head)
    val genLayerPair = genLayers.flatMap(ls => Gen.pick(2, ls)).map { ls => (ls(0), ls(1)) }

    def graph(layer: Layer[GBK]) =
      if (layer.nodes.isEmpty) "empty layer - shouldn't happen"
      else                     layer.nodes.map(showGraph).mkString("\nshowing graphs for layer\n", "\n", "\n")
  }
  trait MscrsDefinition extends CompNodes with TopologicalSort[GroupByKey[_,_]] {
    def selectNode(n: CompNode) = !Seq(n).collect(isAGroupByKey).isEmpty
  }

}


trait TopologicalSort[T <: CompNode] extends CompNodes with Attribution {
  def selectNode(n: CompNode): Boolean

  lazy val selected: CompNode => Boolean = attr { case n => selectNode(n) }

  lazy val layers: CompNode => Set[Layer[T]] = circular(Set[Layer[T]]()) {
    case n if n -> selected => (n -> inputsOutputs).flatMap(_ -> layers) + (n.asInstanceOf[T] -> layer)
    case n                  => (n -> inputsOutputs).flatMap(_ -> layers)
  }

  lazy val layer: T => Layer[T] = attr {
    case n  => Layer((n -> allTargetNodes).toSeq.filterNot(_ -> isStrictParentOf(n)):_*)
  }

  lazy val allTargetNodes: CompNode => Set[T] = circular(Set[T]()) {
    case n if n -> selected => (n -> inputsOutputs).flatMap(_ -> allTargetNodes) + (n.asInstanceOf[T])
    case n                  => (n -> inputsOutputs).flatMap(_ -> allTargetNodes)
  }

  case class Layer[+T](nodes: T*)

}

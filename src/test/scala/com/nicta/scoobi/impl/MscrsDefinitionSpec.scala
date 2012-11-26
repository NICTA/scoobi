package com.nicta.scoobi
package impl

import org.kiama.attribution.Attribution
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import org.specs2._
import plan.mscr._
import plan.mscr.GbkOutputChannel
import plan.mscr.IdInputChannel
import specification.Groups
import matcher.ThrownExpectations
import plan.comp._
import core._
import testing.UnitSpecification
import collection._
import collection.IdSet._
import scala.collection.immutable.SortedSet
import control.Functions._
import scala.xml.factory.NodeFactory
import scala.Some

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
                                                                                         endp^
    "Input channels"                                                                     ^
      "GbkOutputChannels have inputs, some of them are Mappers"                          ^
      "all mappers sharing the same input go to the same MapperInputChannel"             ! g3().e1^
      "other mappers go to an individual MapperInputChannel"                             ! g3().e2^
      "other Gbk inputs go to an IdInputChannel"                                         ! g3().e3^
                                                                                         endp^
    "Mscr creation"                                                                      ^
      "GbkOutputChannels sharing the same MapperInputChannels belong to the same Mscr"   ! g4().e4^
                                                                                         end


  "topological sort of Gbk layers" - new g1 with definition { import scalaz.Scalaz._

    e1 := prop { layer: Layer[GBK] =>
      val nodes = layer.nodes

      nodes must not(beEmpty)
      nodes.forall(n => !nodes.exists(_ -> isStrictParentOf(n))) ==== true
    }.set(minTestsOk -> 100)

    e2 := forAll(genLayerPair) { (pair: (Layer[GBK], Layer[GBK])) => val (layer1, layer2) = pair
      val pairs = ^(layer1.nodes.toStream, layer2.nodes.toStream)((_,_))
      val parentChild = pairs.find { case (n1, n2) => !(n1 -> isStrictParentOf(n2)) }
      lazy val showParentChild = parentChild.collect { case (n1, n2) => (showGraph(n1)+"\n"+showGraph(n2)) }.getOrElse("")

      parentChild aka showParentChild must beNone

    }.set(minTestsOk -> 10, maxDiscarded -> 50)

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

  "Input channels" - new g3 with definition with simpleGraph {

    e1 := {
      val graph = flatten(gbk1, gbk2)
      val ls    = layers(graph)
      val inputChannels: Seq[MapperInputChannel] = mapperInputChannels(ls.head).toSeq

      inputChannels must have size(1)
      inputChannels.head.parDos must have size(2)
    }

    e2 := {
      val graph = flatten(gbk1, gbk2, gbk3)
      val ls    = layers(graph)
      val inputChannels: Seq[MapperInputChannel] = mapperInputChannels(ls.head).toSeq.sortBy(_.parDos.size).reverse

      inputChannels must have size(2)
      inputChannels.head.parDos must have size(2)
      inputChannels.last.parDos must have size(1)
    }

    e3 := {
      val graph = flatten(gbk1, gbk2, gbk3, gbk4)
      val ls    = layers(graph)
      val channels: Seq[InputChannel] = inputChannels(ls.head).toSeq

      channels must have size(3)
    }
  }

  "Mscr creation" - new g4 with definition with simpleGraph {
    e1 := {
      val graph = flatten(gbk1, gbk2, gbk3, gbk4)
      val ls    = layers(graph)
      (ls.head -> mscrs) must have size(2)
    }
  }

  trait simpleGraph extends nodesFactory {
    val ld1 = load
    val (pd1, pd2, cb1) = (pd(ld1), pd(ld1), cb(load))
    val (gbk1, gbk2, gbk3, gbk4) = (gbk(pd1), gbk(pd2), gbk(pd(load)), gbk(cb1))
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

  trait MscrsDefinition extends CompNodes with Layering {
    type T = GBK

    def selectNode(n: CompNode) = !Seq(n).collect(isAGroupByKey).isEmpty

    lazy val mscrs: Layer[T] => Seq[Mscr] = attr { case layer =>
      layer.nodes.map(gbkOutputChannel).groupBy(_.inputs).map { case (i, o) => Mscr(i, o) }
    }

    lazy val gbkOutputChannel: GBK => GbkOutputChannel = attr { case g  =>
      val flatten = Seq(g.in).collect(isAFlatten).headOption

      (g -> ancestors).toList match {
        case (c @ Combine1(_)) :: (p @ ParallelDo1(_)) :: rest => GbkOutputChannel(g, flatten, combiner = Some(c), reducer = Some(p))
        case (c @ Combine1(_)) :: rest                         => GbkOutputChannel(g, flatten, combiner = Some(c))
        case (p @ ParallelDo1(_)) :: rest                      => GbkOutputChannel(g, flatten, reducer = Some(p))
        case _                                                 => GbkOutputChannel(g)
      }
    }

    lazy val inputChannels: Layer[T] => Set[InputChannel] = attr { case layer =>
      mapperInputChannels(layer) ++ idInputChannels(layer)
    }

    lazy val idInputChannels: Layer[T] => Set[IdInputChannel] = attr { case layer =>
      layerInputs(layer).filter(!isParallelDo).map(i => IdInputChannel(i)).toSet
    }

    lazy val mapperInputChannels: Layer[T] => Set[MapperInputChannel] = attr { case layer =>
      mappers(layer).groupBy(_.in.id).values.map(pds => MapperInputChannel(pds:_*)).toSet
    }

    lazy val mappers: Layer[T] => Seq[ParallelDo[_,_,_]] = attr { case layer =>
      layerInputs(layer).collect(isAParallelDo).filterNot(_ -> isReducer)
    }

    lazy val layerInputs: Layer[T] => Seq[CompNode] = attr { case layer =>
      layer.nodes.toSeq.flatMap(_ -> inputs).flatMap {
        case Flatten1(ins) => ins
        case other         => Seq(other)
      }
    }

    lazy val isReducer: ParallelDo[_,_,_] => Boolean = attr { case pd =>
      (pd -> descendents).collect(isAGroupByKey).map(gbkOutputChannel).exists(_.reducer == Some(pd))
    }

  }
}

/**
 * Simple layering algorithm using the Longest path method to assign nodes to layers.
 *
 * See here for a good overview: http://www.cs.brown.edu/~rt/gdhandbook/chapters/hierarchical.pdf
 *
 * In our case the layers have minimum height and possibly big width which is actually good if we run things in parallel
 */
trait Layering extends CompNodes with Attribution with ShowNode {

  type T <: CompNode

  /** a function to select only some nodes in the graph. They must be of type T */
  def selectNode(n: CompNode): Boolean

  lazy val selected: CompNode => Boolean = attr { case n => selectNode(n) }
  lazy val select: PartialFunction[CompNode, T] = { case n if n -> selected => n.asInstanceOf[T] }
  lazy val selectedDescendents: CompNode => Seq[T] = attr { case n => (n -> descendents).toSeq.collect(select) }

  lazy val layers: CompNode => Seq[Layer[T]] = attr { case n =>
    val (leaves, nonLeaves) = selectedDescendents(n).partition(d => selectedDescendents(d).isEmpty)
    Layer.create(leaves) +:
    nonLeaves.groupBy(_ => longestPathTo(leaves)).values.map(Layer.create).toSeq
  }

  lazy val longestPathTo: Seq[CompNode] => CompNode => Int = paramAttr { (target: Seq[CompNode]) => node: CompNode =>
    target.map(t => node -> longestPathToNode(t)).max
  }

  lazy val longestPathToNode: CompNode => CompNode => Int = paramAttr { (target: CompNode) => node: CompNode =>
    if (node.id == target.id)                0  // found
    else if (node.children.asNodes.isEmpty) -1 // not found
    else                                     1 + (node.children.asNodes).map(_ -> longestPathToNode(target)).max
  }

  case class Layer[T <: CompNode](nodes: SortedSet[T] = IdSet.empty)
  object Layer {
    def create[T <: CompNode](ts: Seq[T]) = Layer(collection.IdSet(ts:_*))
  }
}

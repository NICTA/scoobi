package com.nicta.scoobi
package impl

import org.specs2._
import specification._
import matcher.MustMatchers
import Scoobi._
import plan.mscr._
import plan.comp._
import com.nicta.scoobi.core.{ProcessNode, CompNode}
import control.Functions._
import org.specs2.control.Debug
import CollectFunctions._
import org.scalacheck.Prop._
import scalaz.syntax.apply._
import scalaz.std.stream._
import org.scalacheck.{Gen, Arbitrary}

class MscrsDefinitionSpec extends script.Specification with Groups with CompNodeData { def is = s2"""

 The algorithm for making mscrs works by:

  - creating layers of independent nodes
  - creating mscrs for each layer

Layers
======

  1. divide the computation graph into "layers" of nodes where all the nodes on a given layer are independent from each other
    + all the nodes in a layer cannot be parent of one another
    + 2 different layers have at least 2 nodes parent of each other
    + example of layers on a simple graph

  2. re-group those layers so that each group ends-up with "output nodes"
    + an output node is a node where an output needs to be done: materialised or gbk output or end of the graph or checkpoint
      (but not a return node or a load node, or a materialise node)
    + regroup all the layers into bigger layers with output nodes
    + if a layer contains both output nodes and non-ouput nodes, the non-output nodes must be pushed to "lower" layers

Mscrs
=====

  3. create mscrs for each layer
    + first get the source nodes
     + source nodes are the output of the layer above
     + source nodes cannot be Return nodes or Op nodes (because this input is retrieved via environments)
    + then create all the gbk channels from the source nodes
      + if there are no gbks, there must be no channel
    + and create all the "floating parallel do" channels from the source nodes
    + then create the output channels for the gbks
    + and create the output channels for the "floating parallel do nodes"
      + if a mapper is not only connected to a gbk, it must have its own output channel
    + group the input channels by common input
    + assemble the mscrs with input and output channels

Robustness
==========

  + A ProcessNode can only belong to one and only one channel and all ProcessNodes must belong to a channel
    except if it is a flatten node


"""

  "layers" - new group with definition with CompNodeFactory with someLists with layers {

    eg := prop { layer: Layer[CompNode] =>
      val nodes = layer.nodes
      nodes.forall(n => !nodes.exists(_ -> isStrictParentOf(n)))
    }

    eg := forAll(genLayerPair) { (pair: (Layer[CompNode], Layer[CompNode])) => val (layer1, layer2) = pair
      val pairs = ^(layer1.gbks.toStream, layer2.gbks.toStream)((_,_))

      forallWhen(pairs) { case (n1, n2) =>
        (n1 -> isStrictParentOf(n2)) aka (showGraph(n1)+"\n"+showGraph(n2)) must beTrue
      }

    }.set(maxSize = 6, maxDiscardRatio = 7f)

    eg := layersOf(Seq(optimise(twoLayersList.getComp))) must haveSize(11)

    eg := {
      val (materialised, gbkOutput, endNode, withCheckpoint, returnNode) =
        (pd(), pd(gbk(pd())), aRoot(load), DList(1).checkpoint("path")(ScoobiConfiguration()).getComp, rt)
      initTree(aRoot(mt(materialised), gbkOutput, endNode, withCheckpoint))

      (Seq(materialised, gbkOutput, endNode, withCheckpoint) must contain(isAnOutputNode).forall) and
      (Seq(returnNode) must not(contain(isAnOutputNode)))
    }

    eg := partitionLayers(twoLayersList) must haveSize(3)

    eg := partitionLayers(twoLayersList)(2).nodes must contain(isParallelDo)
  }

  "mscrs" - new group with definition with someLists with Debug {

    eg := partitionLayers(twoLayersList).map(_.nodes).map(inputNodes _).map(_.size) must_== Seq(0, 1, 1)

      eg := inputNodes(partitionLayers(twoGroupByKeys)(2).nodes).filter(isCombine) must haveSize(1)

      eg := partitionLayers(twoLayersList).map(_.nodes).flatMap(inputNodes) must not contain(isReturn || isOp)

    eg := gbkInputChannels(partitionLayers(simpleGroupByKeyList)(1)) must haveSize(1)

      eg := gbkInputChannels(partitionLayers(simpleList)(1)) must beEmpty

    eg := floatingInputChannels(partitionLayers(simpleList.map(identity))(1)) must haveSize(1)

    eg := gbkOutputChannels(partitionLayers(simpleList.groupByKey)(1)) must haveSize(1)

    eg := floatingOutputChannels(partitionLayers(simpleList.map(identity))(1)) must haveSize(1)

      eg := outputChannels(partitionLayers(twoLayersList).pp.apply(1)) must haveSize(2)

    eg := groupInputChannels(partitionLayers(twoIndependentGroupByKeys)(1)).head must haveSize(2)

    eg := mscrs(partitionLayers(twoLayersList).pp.apply(1)) must haveSize(1)

  }

  "robustness" - new group with definition with Optimiser with Debug {
    eg := prop { (l1: DList[String]) =>
      val listNodes = optimise(l1.getComp)
      val layers = createMapReduceLayers(listNodes)
      val channels = layers.flatMap(mscrs).flatMap(_.channels).distinct

      def print =
        "\nOPTIMISED\n"+pretty(listNodes) +
        "\nLAYERS\n"+layers.mkString("\n") +
        "\nCHANNELS\n"+channels.mkString("\n")

      processNodes(listNodes) must contain { (n: CompNode) =>
        val nodeCountInChannels = channels.filter(_.processNodes.contains(n)) aka (print+"\nFOR NODE\n"+n)
        if (isFlatten(n)) nodeCountInChannels must be_>=(1) ^^ ((_: Seq[_]).size)
        else              nodeCountInChannels must haveSize(1)
      }.forall
    }.set(minTestsOk = 100)

    lazy val processNodes: CompNode => Seq[CompNode] = attr {
      case node: ProcessNode => Seq(node) ++ children(node).flatMap(processNodes)
      case node              => children(node).flatMap(processNodes)
    }

    def isFlatten(node: CompNode) = node match {
      case ParallelDo1(ins) if ins.size > 1 => true
      case other                            => false
    }
  }

  trait definition extends MscrsDefinition with MustMatchers with Debug {
    // for testing
    def makeLayers(start: CompNode): Seq[Layer[T]] =
      layersOf(Seq(optimise(start)))

    def partitionLayers(list: DList[_]) =
      super.createMapReduceLayers(optimise(list.getComp).pp(pretty _))
  }


  trait someLists extends Optimiser with ShowNode with Debug {
    lazy val simpleList = DList((1, 2))

    lazy val simpleGroupByKeyList = DList((1, 2)).groupByKey

    lazy val twoIndependentGroupByKeys = {
      val (l1, l2) = (DList((1, 2)), DList((1, 2)))
      val (l3, l4) = (l1 ++ l2, l1 ++ l2)
      val (l5, l6) = (l3.groupByKey, l4.groupByKey)
      l5 ++ l6
    }

    lazy val twoGroupByKeys = DList((1, "1")).filter(_ => true).groupByKey.combine(Reduction.first).groupByKey.filter(_ => true)

    lazy val twoLayersList = {
      lazy val dlist = DList(1, 2, 3, 4).filter(_ % 2 == 0)
      dlist.size join dlist.map(identity)
    }
  }

  trait layers extends Layering with CompNodeData {
    implicit val arbitraryLayer: Arbitrary[Layer[CompNode]] = Arbitrary(genLayer)

    // make sure there is at least one layer
    // by construction, there is no cycle
    val genLayers = arbitraryCompNode.arbitrary.map { n =>
      resetMemo()             // reset the memos otherwise too much data accumulates during testing!
      layersOf(Seq(gbk(pd(gbk(n))))) // generate at least 2 layers
    }
    val genLayer     = genLayers.flatMap(ls => Gen.pick(1, ls)).map(_.head)
    val genLayerPair = genLayers.flatMap(ls => Gen.pick(2, ls)).map(ls => (ls(0), ls(1))).filter { case (l1, l2) => l1 != l2 }

    def graph(layer: Layer[CompNode]) =
      if (layer.nodes.isEmpty) "empty layer - shouldn't happen"
      else                     layer.nodes.map(showGraph).mkString("\nshowing graphs for layer\n", "\n", "\n")

  }
}


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
/*
class MscrsDefinitionSpec extends UnitSpecification with Groups with ThrownExpectations { def is = s2"""

  The outputs of a graph can be sorted in layers according to their dependencies

    all the nodes in a layer cannot be parent of each other                                                             ${g1().e1}
    2 different layers have at least 2 nodes parent of each other                                                       ${g1().e2}
    The following nodes must be selected to create layers
      the last ParallelDo of a graph                                                                                    ${g2().e1}
      the last ParallelDo of a graph when it is a flatten node                                                          ${g2().e2}
      the last GroupByKey of a graph                                                                                    ${g2().e3}
      the last Combine of a graph                                                                                       ${g2().e4}
      the Root node                                                                                                     ${g2().e5}

  For each layer in the topological sort, we can create Mscrs
    Output channels                                                                                                     ${section("outputs")}
      each gbk belongs to a GbkOutputChannel                                                                            ${g3().e1}
      aggregating the combine node if there is one after the gbk                                                        ${g3().e2}
      aggregating the pd node if there is one after the gbk                                                             ${g3().e3}
      aggregating the combine and pd nodes if they are after the gbk                                                    ${g3().e4}
                                                                                                                        ${ section("outputs")}
    Input channels
      all mappers sharing the same input go to the same MscrInputChannel                                                ${g4().e1}

    Mscr creation                                                                                                       ${section("creation")}
      there must be one mscr per set of related tags                                                                    ${g5().e1}
                                                                                                                        """


  "layering of layers according to their outputs" - new g1 with definition {
    import scalaz.Scalaz._

    e1 := prop { layer: Layer[CompNode] =>
      val nodes = layer.nodes
      nodes.forall(n => !nodes.exists(_ -> isStrictParentOf(n)))
    }.set(minTestsOk = 100)

    e2 := forAll(genLayerPair) { (pair: (Layer[CompNode], Layer[CompNode])) => val (layer1, layer2) = pair
      val pairs = ^(layer1.gbks.toStream, layer2.gbks.toStream)((_,_))

      forallWhen(pairs) { case (n1, n2) =>
        (n1 -> isStrictParentOf(n2)) aka (showGraph(n1)+"\n"+showGraph(n2)) must beTrue
      }

    }.set(minTestsOk = 100, maxSize = 6, maxDiscardRatio = 7f)

  }

  "selected nodes" - new g2 with definition {
    e1 := {
      val pd1 = pd(load)
      layers(pd1).last.nodes must contain(pd1)
    }
    e2 := {
      val pd1 = pd(pd(load), pd(load))
      val ls = layers(pd1)
      ls must have size(1)
      ls.head.nodes must contain(pd1)
    }
    e3 := {
      val gbk1 = gbk(load)
      layers(gbk1).last.nodes must contain(gbk1)
    }
    e4 := {
      val cb1 = cb(load)
      layers(cb1).last.nodes must contain(cb1)
    }
    e5 := {
      val root1 = aRoot(pd(load))
      layers(root1).last.nodes must contain(root1)
    }
  }

  "Output channels" - new g3 with definition {

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

  "Input channels" - new g4 with definition {
    e1 := {
      val l1 = load
      val (pd1, pd2, pd3) = (pd(l1), pd(l1), pd(l1))
      val (gbk1, gbk2, gbk3) = (gbk(pd1), gbk(pd2), gbk(pd3))
      val layer1    = layers(aRoot(gbk1, gbk2, gbk3)).head

      gbkInputChannels(layer1) must  have size(1)
    }
  }

  "Mscr creation" - new g5 with definition {

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
*/
package com.nicta.scoobi
package impl

import org.specs2._
import specification._
import org.specs2.matcher.{ThrownExpectations, MustMatchers}
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
import com.nicta.scoobi.testing.script.UnitSpecification

class MscrsDefinitionSpec extends UnitSpecification with Groups { def is = s2"""

 The algorithm for making mscrs works by:

 1. building layers of independent nodes in the graph
 1. finding the input nodes for the first "process" layer
 1. reaching "output" nodes from the input nodes to create the "InputOutput layer"
 1. building output channels with those nodes
 1. building input channels connecting the output to the input nodes
 1. aggregating input and output channels as Mscr representing a full map reduce job
 1. iterating on any processing node that is not part of a Mscr

Layers
======

 Divide the computation graph into "layers" of nodes where all the nodes on a given layer are independent from each other
  + all the nodes in a layer cannot be parent of one another
  + 2 different layers have at least 2 nodes parent of each other

Process layer and input nodes
=============================
  + the "processLayers" method returns layers of non-visited processing nodes
  + the "inputNodes" of a given group of nodes are non-value nodes outside the group which are directly connected to the group

Output nodes
============

  + an "output node" is a node where an output needs to be done: materialised or gbk output or end of the graph or checkpoint
    (but not a return node or a load node, or a materialise node)
  + the "input/output layer" contains all the nodes connected to input nodes up to the first output nodes
    it must not contain already visited nodes

Output channels
===============

 Gbk output channels are created from the gbk belonging to the input/output layer
  + each gbk of the layer belongs to one and only one GbkOutputChannel
  + the output channel must contain the combine node if there is one after the gbk (even if it is not part of the layer)
  + the output channel must contain the pd node if there is one after the gbk (even if it is not part of the layer)
  + the output channel must contain the combine and pd nodes if they are after the gbk (even if not part of the layer)

 Bypass output channels are created from the "bypass mappers" of input channels
  + last mappers of gbk input channels not being used only by gbks
  + last mappers of floating input channels


Input channels
===============

 + Gbk input channels are built by collecting all the nodes in between *one* input node and gbks on the layer

 + Floating input channels are built by finding the "last floating" mappers. Those are mappers of the layer which are not
   mappers from a Gbk input channel and which are used outside of the layer

Mscrs
=====

 The mscrs are created by
  + grouping all the input channels when they have at least one output tag in common into the same mscr
  + adding the corresponding output channels (the ones having the same output tag) to the mscr


Robustness
==========

  + A ProcessNode can only belong to one and only one channel and all ProcessNodes must belong to a channel
    except if it is a flatten node
"""

  "layers" - new group with definition with layers with CompNodeData {
    eg := prop { layer: Seq[CompNode] =>
      layer.forall(n => !layer.exists(_ -> isStrictParentOf(n)))
    }

    eg := forAll(genLayerPair) { (pair: (Seq[CompNode], Seq[CompNode])) => val (layer1, layer2) = pair
      val pairs = ^(layer1.collect(isAGroupByKey).toStream, layer2.collect(isAGroupByKey).toStream)((_,_))

      forallWhen(pairs) { case (n1, n2) =>
        (n1 -> isStrictParentOf(n2)) aka (showGraph(n1)+"\n"+showGraph(n2)) must beTrue
      }

    }.set(maxSize = 6, maxDiscardRatio = 7f)
  }

  "input nodes" - new group with definition with factory {
    eg := {
      val pd0 = pd(load); val pd1 = pd(pd0); val pd2 = pd(pd1)
      processLayers(Seq(pd0, pd1, pd2, load, op(load, load), mt(pd1), rt), visited = Seq(pd0)) ===
        Seq(Seq(pd1), Seq(pd2))
    }

    eg := "there are 2 inputs for the flatten node" ==> {
      val pd1 = pd(load, load); val pd2 = pd(pd1)
      inputNodes(Seq(pd1, pd2)) must haveSize(2)
    }
  }

  "Input/Output layer" - new group with definition with factory with ThrownExpectations {
    eg := "some nodes only are output nodes" ==> {
      val (materialised, gbkOutput, endNode, withCheckpoint, returnNode) =
        (pd(), pd(gbk(pd())), aRoot(load), DList(1).checkpoint("path")(ScoobiConfiguration()).getComp, rt)

      Seq(materialised, gbkOutput, endNode, withCheckpoint) must contain(isAnOutputNode).forall
      Seq(returnNode) must not(contain(isAnOutputNode))
    }

    eg := "this input-output layer contains only the nodes between the input and the first output" ==> {
      // load -> pd -> pd -> pd -> gbk -> cb -> gbk
      val pd0 = pd(load); val pd1 = pd(pd0); val pd2 = pd(pd1); val gbk1 = gbk(pd2); val cb1 = cb(gbk1); val gbk2 = gbk(cb1)
      // gbk2 cannot be part of the layer because it depends on a node that is already part of it
      createInputOutputLayer(Seq(pd0), Seq(pd0)) must contain(exactly[CompNode](pd1, pd2, gbk1))
    }
  }

  "Output channels" - new group with definition with factory {

    eg := {
      val (gbk1, gbk2, gbk3) = (gbk(load), gbk(load), gbk(load))
      outputChannels(Seq(gbk1, gbk2, gbk3)) === Seq(GbkOutputChannel(gbk1), GbkOutputChannel(gbk2), GbkOutputChannel(gbk3))
    }
    eg := {
      val gbk1 = gbk(load)
      outputChannels(Seq(gbk1)) === Seq(GbkOutputChannel(gbk1))
    }
    eg := {
      val gbk1 = gbk(load); val cb1 = cb(gbk1)
      outputChannels(Seq(gbk1)) === Seq(GbkOutputChannel(gbk1, combiner = Some(cb1)))
    }
    eg := {
      val gbk1 = gbk(load); val pd1 = pd(gbk1)
      outputChannels(Seq(gbk1)) === Seq(GbkOutputChannel(gbk1, reducer = Some(pd1)))
    }
    eg := {
      val gbk1 = gbk(load); val cb1 = cb(gbk1); val pd1 = pd(cb1)
      outputChannels(Seq(gbk1)) === Seq(GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(pd1)))
    }
    eg := "there is one bypass output channel for pd2" ==> {
      val l1 = load
      val (pd1, pd2, pd3) = (pd(l1), pd(l1), pd(load))
      val (gbk1, gbk2, gbk3) = (gbk(pd1), gbk(pd2), gbk(pd3))
      aRoot(mt(pd2), gbk1, gbk2, gbk3)
      bypassOutputChannels(Seq(pd1, pd2, pd3, gbk1, gbk2, gbk3)) must haveSize(1)
    }
    eg := "there is one bypass output channel for pd4" ==> {
      val l1 = load
      val (pd1, pd2, pd3) = (pd(l1), pd(l1), pd(load)); val pd4 = pd(pd1)
      val (gbk1, gbk2, gbk3) = (gbk(pd1), gbk(pd2), gbk(pd3))
      aRoot(mt(pd4), gbk1, gbk2, gbk3)
      bypassOutputChannels(Seq(pd1, pd2, pd3, pd4, gbk1, gbk2, gbk3)) must haveSize(1)
    }
  }

  "Input channels" - new group with definition with factory {
    eg := {
      val l1 = load
      val (pd1, pd2, pd3) = (pd(l1), pd(l1), pd(load))
      val (gbk1, gbk2, gbk3) = (gbk(pd1), gbk(pd2), gbk(pd3))
      inputChannels(Seq(pd1, pd2, pd3, gbk1, gbk2, gbk3)) must haveSize(2)
    }
    eg := "there is a FloatingInput channel for the materialised mapper" ==> {
      val l1 = load
      val (pd1, pd2, pd3) = (pd(l1), pd(l1), pd(load)); val pd4 = pd(pd1)
      val (gbk1, gbk2, gbk3) = (gbk(pd1), gbk(pd2), gbk(pd3))
      aRoot(mt(pd4), gbk1, gbk2, gbk3)
      inputChannels(Seq(pd1, pd2, pd3, pd4, gbk1, gbk2, gbk3)) must haveSize(3)
    }
  }

  "mscrs creation" - new group with definition with factory {
    val (l1, l2) = (load, load)
    val (pd1, pd2) = (pd(l1), pd(l2)); val pd3 = pd(pd1, pd2); val pd4 = pd(load)
    val (gbk1, gbk2, gbk3) = (gbk(pd3), gbk(pd3), gbk(pd4))
    aRoot(gbk1, gbk2, gbk3)

    eg := "there is one mscr with 2 input channels and one mscr with 1 input channel" ==> {
      createMscrs(Seq(pd1, pd2, pd3, gbk1, gbk2, gbk3)).mscrs.map(_.inputChannels.size) === Seq(2, 1)
    }
    eg := "there is one mscr with 2 output channels and one mscr with 1 output channel" ==> {
      createMscrs(Seq(pd1, pd2, pd3, gbk1, gbk2, gbk3)).mscrs.map(_.outputChannels.size) === Seq(2, 1)
    }
  }

  "robustness" - new group with definition with Debug with CompNodeData with factory {
    eg := prop { (l1: DList[String]) =>
      val start = optimise(l1.getComp)
      val mscrLayers = createMapReduceLayers(start)

      // for each process node in the graph count how many times it is represented in a channel
      processNodes(start) must contain { n: CompNode =>
        val nodeCountInChannels = count(mscrLayers, start, n)

        if (isFlatten(n)) nodeCountInChannels must be_>=(1) ^^ ((_: Seq[_]).size)
        else              nodeCountInChannels must haveSize(1)
      }.forall

    }.set(minTestsOk = 1000)
  }

  trait definition extends MscrsDefinition with MustMatchers with Debug {
    // for testing
    def makeLayers(start: CompNode): Seq[Seq[CompNode]] =
      layersOf(Seq(optimise(start)))

    def processLayers(list: DList[_]) =
      super.processLayers(Seq(optimise(list.getComp)), visited = Seq())

    def createMapReduceLayers(list: DList[_]) =
      super.createMapReduceLayers(optimise(list.getComp))

    lazy val processNodes: CompNode => Seq[CompNode] = attr {
      case node: ProcessNode => Seq(node) ++ children(node).flatMap(processNodes)
      case node              => children(node).flatMap(processNodes)
    }

    def isFlatten(node: CompNode) = node match {
      case ParallelDo1(ins) if ins.size > 1 => true
      case other                            => false
    }

    def count(layers: Seq[Layer], start: CompNode, n: CompNode) = {
      val channels = layers.flatMap(_.mscrs).flatMap(_.channels).distinct

      def print =
        (Seq("OPTIMISED", pretty(start)) ++
         Seq("LAYERS")   ++ layers ++
         Seq("CHANNELS") ++ channels).mkString("\n\n")

      channels.filter(_.processNodes.contains(n)) aka (print+"\nFOR NODE\n"+n)
    }
  }

  trait layers extends Layering with CompNodeData {
    implicit val arbitraryLayer: Arbitrary[Seq[T]] = Arbitrary(genLayer)

    // make sure there is at least one layer
    // by construction, there is no cycle
    val genLayers = arbitraryCompNode.arbitrary.map { n =>
      resetMemo()             // reset the memos otherwise too much data accumulates during testing!
      layersOf(Seq(gbk(pd(gbk(n))))) // generate at least 2 layers
    }
    val genLayer     = genLayers.flatMap(ls => Gen.pick(1, ls)).map(_.head)
    val genLayerPair = genLayers.flatMap(ls => Gen.pick(2, ls)).map(ls => (ls(0), ls(1))).filter { case (l1, l2) => l1 != l2 }
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
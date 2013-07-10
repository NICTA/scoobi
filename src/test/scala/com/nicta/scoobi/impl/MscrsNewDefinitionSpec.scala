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

class MscrsNewDefinitionSpec extends script.Specification with Groups with CompNodeData { def is = s2"""

 The algorithm for making mscrs works by:

  - creating layers of independent nodes
  - creating mscrs for each layer

Layers
======

  1. divide the computation graph into "layers" of nodes where all the nodes on a given layer are independent from each other
    + make layers of independent nodes

  2. re-group those layers so that each group ends-up with "output nodes"
    + an output node is a node where an output needs to be done: materialised or gbk output or end of the graph or checkpoint
      (but not a return node or a load node, or a materialise node)
    + regroup all the layers into bigger layers with output nodes
    + if a layer contains both output nodes and non-ouput nodes, the non-output nodes must be transferred to "lower" layers

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

  "layers" - new group with definition with CompNodeFactory with someLists {

    eg := layers(optimise(twoLayersList.getComp)) must haveSize(11)

    eg := {
      val (materialised, gbkOutput, endNode, withCheckpoint, returnNode) =
        (pd(), pd(gbk(pd())), aRoot(load), DList(1).checkpoint("path")(ScoobiConfiguration()).getComp, rt)
      initTree(aRoot(mt(materialised), gbkOutput, endNode, withCheckpoint))

      (Seq(materialised, gbkOutput, endNode, withCheckpoint) must contain(isAnOutputNode).forall) and
      (Seq(returnNode) must not(contain(isAnOutputNode)))
    }

    eg := partitionLayers(twoLayersList) must haveSize(3)

    eg := pending
  }

  "mscrs" - new group with definition with someLists with Debug {

    eg := partitionLayers(twoLayersList).map(inputNodes _).map(_.size) must_== Seq(0, 1, 1)

      eg := inputNodes(partitionLayers(twoGroupByKeys)(2)).filter(isCombine) must haveSize(1)

      eg := partitionLayers(twoLayersList).flatMap(inputNodes) must not contain(isReturn || isOp)

    eg := gbkInputChannels(partitionLayers(simpleGroupByKeyList)(1)) must haveSize(1)

      eg := gbkInputChannels(partitionLayers(simpleList)(1)) must beEmpty

    eg := floatingInputChannels(partitionLayers(simpleList.map(identity))(1)) must haveSize(1)

    eg := gbkOutputChannels(partitionLayers(simpleList.groupByKey)(1)) must haveSize(1)

    eg := floatingOutputChannels(partitionLayers(simpleList.map(identity))(1)) must haveSize(1)

      eg := outputChannels(partitionLayers(twoLayersList)(1)) must haveSize(2)

    eg := groupInputChannels(partitionLayers(twoIndependentGroupByKeys)(1)).head must haveSize(2)

    eg := mscrs(partitionLayers(twoLayersList)(1)) must haveSize(1)

  }

  "robustness" - new group with definition with Optimiser with Debug {
    eg := prop { (l1: DList[String]) =>
      val listNodes = optimise(l1.getComp)
      val layers = partitionLayers(listNodes)
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

  trait definition extends MscrsDefinition2 with MustMatchers {
    // for testing
    def makeLayers(start: CompNode): Seq[Layer[T]] =
      layers(optimise(start))

    def partitionLayers(list: DList[_]) =
      super.partitionLayers(optimise(list.getComp))
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

    lazy val twoGroupByKeys = DList((1, "1")).filter(_ => true).groupByKey.combine(Reduction.first).groupByKey.filter(_ => true)//DList((1, 2)).groupByKey.combine(Reduction.Sum.int).groupByKey

    lazy val twoLayersList = {
      lazy val dlist = DList(1, 2, 3, 4).filter(_ % 2 == 0)
      dlist.size join dlist.map(identity)
    }
  }

}


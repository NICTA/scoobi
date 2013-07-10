package com.nicta.scoobi
package impl
package plan
package mscr

import core._
import collection._
import Seqs._
import comp._
import CompNodes._
import control.Functions._

trait MscrsDefinition2 extends Layering with Optimiser with ShowNode {

  def partitionLayers(start: CompNode): Seq[Layer[T]] = {
    def partition(layers: Seq[Layer[T]]): Seq[Layer[T]] =
      if (layers.isEmpty) Seq()
      else {
        val (first, rest) = layers.span(_.nodes.forall(!isAnOutputNode))
        val (outputNodes, noOutputNodes) = rest.headOption.map { l =>
          val (out, other) = l.nodes.partition(isAnOutputNode)
          (Layer(out), Layer(other))
        }.getOrElse((Layer[T](), Layer[T]()))

        if (outputNodes.isEmpty) fuse(first :+ noOutputNodes) +: partition(rest.drop(1))
        else                     fuse(first :+ outputNodes) +: partition(noOutputNodes +: rest.drop(1))
      }
    partition(layers(start)).filterNot(_.isEmpty)
  }

  def fuse(layers: Seq[Layer[T]]) = {
    Layer(layers.flatMap(_.nodes.filter(isProcessNode || isLoad)))
  }

  def mscrs(layer: Layer[T]): Seq[Mscr] = {
    val outChannels = outputChannels(layer)
    val channelsWithCommonTags = groupInputChannels(layer)
    // create Mscr for each set of channels with common tags
    channelsWithCommonTags.map { taggedInputChannels =>
      val correspondingOutputTags = taggedInputChannels.flatMap(_.tags)
      val out = outChannels.filter(o => correspondingOutputTags.contains(o.tag))

      if (out.isEmpty) Mscr.empty
      else             Mscr.create(taggedInputChannels, out)
    }.filterNot(_.isEmpty)
  }

  def groupInputChannels(layer: Layer[T]) =
    Seqs.transitiveClosure(inputChannels(layer)) { (i1: InputChannel, i2: InputChannel) =>
      i1.sourceNode == i2.sourceNode
    }.map(_.list)

  def inputChannels(layer: Layer[T]) = gbkInputChannels(layer) ++ floatingInputChannels(layer)

  def inputNodes(layer: Layer[T]): Seq[CompNode] =
    layer.nodes.filterNot(isReturn || isOp).collect {
      case node if children(node).exists(!layer.nodes.contains(_)) => children(node).filterNot(n => layer.nodes.contains(n) || isReturn(n) || isOp(n))
    }.distinct.flatten

  def hasSinkNode: Layer[T] => Boolean = (layer: Layer[T]) => {
    layer.nodes.exists(isAnOutputNode)
  }

  def gbkInputChannels(layer: Layer[T]): Seq[GbkInputChannel] = {
    val gbks = layer.nodes.filter(isGroupByKey)
    val in = inputNodes(layer)
    in.flatMap { inputNode =>
      val groupByKeyUses = transitiveUses(inputNode).collect(isAGroupByKey).filter(gbks.contains).toSeq
      if (groupByKeyUses.isEmpty) Seq()
      else                        Seq(new GbkInputChannel(inputNode, groupByKeyUses, this))
    }
  }

  def floatingInputChannels(layer: Layer[T]): Seq[FloatingInputChannel] = {
    val gbks = gbkInputChannels(layer).flatMap(_.groupByKeys)
    val inputs = inputNodes(layer)

    inputs.map { inputNode =>
      val mappers = transitiveUses(inputNode)
        .collect(isAParallelDo)
        .filter(layer.nodes.contains)
        .filterNot(isReducer)
        .filterNot(n => uses(n).nonEmpty && uses(n).forall(gbks.contains)).toSeq
      new FloatingInputChannel(inputNode, mappers, this)
    }
  }

  def gbkOutputChannels(layer: Layer[T]) = {
    val gbks = layer.nodes.collect(isAGroupByKey)
    gbks.map(gbk => gbkOutputChannel(gbk))
  }

  /**
   * @return a gbk output channel based on the nodes which are following the gbk
   */
  def gbkOutputChannel(gbk: GroupByKey): GbkOutputChannel = {
    parents(gbk) match {
      case (c: Combine) +: (p: ParallelDo) +: rest if isReducer(p) => GbkOutputChannel(gbk, combiner = Some(c), reducer = Some(p))
      case (p: ParallelDo) +: rest                 if isReducer(p) => GbkOutputChannel(gbk, reducer = Some(p))
      case (c: Combine) +: rest                                    => GbkOutputChannel(gbk, combiner = Some(c))
      case _                                                       => GbkOutputChannel(gbk)
    }
  }


  def outputChannels(layer: Layer[T]) = gbkOutputChannels(layer) ++ floatingOutputChannels(layer)

  def floatingOutputChannels(layer: Layer[T]) = {
    val floatingMappers = inputChannels(layer).flatMap(_.lastMappers).filter(mapper => uses(mapper).isEmpty || uses(mapper).exists(!isGroupByKey))
    floatingMappers.distinct.map(BypassOutputChannel(_))
  }

  def selectNode: CompNode => Boolean = (n: CompNode) => true

  def isAnInputNode(nodes: Seq[CompNode]) = (node: CompNode) =>
    !isReturn(node) &&
      (children(node).isEmpty || children(node).forall(!nodes.contains(_)))

  def isAnOutputNode = (isMaterialised  || isGbkOutput || isEndNode || isCheckpoint || isLoad) && !isReturn

  /** node at the end of the graph */
  def isEndNode: CompNode => Boolean = attr { n =>
    parent(n).map(isRoot).getOrElse(true)
  }

  def isMaterialised: CompNode => Boolean = attr {
    case n => uses(n).exists(isMaterialise || isOp)
  }

  def isCheckpoint: CompNode => Boolean = attr {
    case p: ProcessNode => p.hasCheckpoint
    case other          => false
  }

  def isGbkOutput: CompNode => Boolean = attr {
    case pd: ParallelDo                       => isReducer(pd)
    case cb @ Combine1(gbk: GroupByKey)       => parent(cb).map(!isReducingNode).getOrElse(true) && isUsedAtMostOnce(gbk)
    case gbk: GroupByKey                      => parent(gbk).map(!isReducingNode).getOrElse(true)
    case other                                => false
  }

  lazy val isReducer: ParallelDo => Boolean = attr {
    case pd @ ParallelDo1((cb @ Combine1((gbk: GroupByKey))) +: rest) => rest.isEmpty && isUsedAtMostOnce(pd) && isUsedAtMostOnce(cb) && isUsedAtMostOnce(gbk)
    case pd @ ParallelDo1((gbk: GroupByKey) +: rest)                  => rest.isEmpty && isUsedAtMostOnce(pd) && isUsedAtMostOnce(gbk)
    case _                                                            => false
  }

  /**
   * a node is said to be reducing if it is in a "reducing chain of nodes" after a gbk
   *
   *  - parallelDo(combine(gbk)) // parallelDo and combine are reducing
   *  - combine(gbk)             // combine is reducing
   *  - parallelDo(gbk)          // parallelDo is reducing
   * @return
   */
  def isReducingNode: CompNode => Boolean = attr {
    case pd: ParallelDo          => isReducer(pd)
    case Combine1(_: GroupByKey) => true
    case other                   => false
  }

  lazy val layerSinks: Layer[T] => Seq[Sink] =
    attr { case layer => mscrs(layer).flatMap(_.sinks).distinct }

}
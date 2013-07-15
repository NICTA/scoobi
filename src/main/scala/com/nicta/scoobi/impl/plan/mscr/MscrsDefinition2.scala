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
import CollectFunctions._

trait MscrsDefinition2 extends Layering with Optimiser with ShowNode {

  def partitionLayers(start: CompNode): Seq[Layer[T]] = {
//    def partition(layers: Seq[Layer[T]]): Seq[Layer[T]] =
//      if (layers.isEmpty) Seq()
//      else {
//        val (first, rest) = layers.span(_.nodes.forall(!isAnOutputNode))
//        val (outputNodes, noOutputNodes) = rest.headOption.map { l =>
//          val (out, other) = l.nodes.partition(isAnOutputNode)
//          val (dependentOut, independentOther) = other.partition(o => transitiveUses(o).exists(out.contains))
//          (Layer(out ++ dependentOut), Layer(independentOther))
//        }.getOrElse((Layer[T](), Layer[T]()))
//
//        if (outputNodes.isEmpty) fuse(first :+ noOutputNodes) +: partition(rest.drop(1))
//        else                     fuse(first :+ outputNodes) +: partition(rest.drop(1).headOption.map(l => fuse(Seq(l, noOutputNodes))).toSeq ++ rest.drop(2))
//      }
//    partition(layers(start)).filterNot(_.isEmpty)

    createLayers(Seq(start)).filterNot(_.isEmpty)
  }

  private def createLayers(start: Seq[CompNode], visited: Seq[CompNode] = Seq()): Seq[Layer[T]] = {
    val allLayers = layersFromNodes(start.distinct).map { layer =>
      Layer(layer.nodes.filterNot(n => isLoad(n) || visited.contains(n)).distinct)
    }.filterNot(_.isEmpty)

    if (allLayers.isEmpty) Seq[Layer[T]]()
    else {
      val firstLayer = allLayers.head
      val layerInputNodes = inputNodes(firstLayer)

      val mscrLayer = Layer(createMscrs(layerInputNodes, visited))
      val remainingNodes = allLayers.flatMap(_.nodes).filterNot(n => mscrLayer.nodes.contains(n) || layerInputNodes.contains(n) || visited.contains(n) || isValueNode(n))
      if (remainingNodes.isEmpty) Seq(mscrLayer)
      else                        mscrLayer +: createLayers(remainingNodes, visited ++ firstLayer.nodes ++ mscrLayer.nodes)
    }
  }

  lazy val mscrs: Layer[T] => Seq[Mscr] = (layer: Layer[T]) => layer.mscrs


  private def createMscrs(inputNodes: Seq[CompNode], visited: Seq[CompNode]): Seq[Mscr] = {

    // create paths from each input node to an output node
    val layer = createLayer(inputNodes, visited)
    val outChannels = outputChannels(layer)
    val channelsWithCommonTags = groupInputChannels(layer)
    // create Mscr for each set of channels with common tags
    channelsWithCommonTags.map { taggedInputChannels =>
      val correspondingOutputTags = taggedInputChannels.flatMap(_.tags)
      val out = outChannels.filter(o => correspondingOutputTags.contains(o.tag))

      Mscr.create(taggedInputChannels, out)
    }.filterNot(_.isEmpty)
  }

  private def createLayer(inputNodes: Seq[CompNode], visited: Seq[CompNode]): Layer[T] = {
    val layerNodes = transitiveUsesUntil(inputNodes, isAnOutputNode)
    val gbks = layerNodes.filter(isAnOutputNode).filterNot(visited.contains)
    val gbkLayers = layersOf(gbks, isAnOutputNode)
    val gbkFirst = gbkLayers.dropWhile(l => !l.nodes.exists(gbks.contains)).headOption.map(_.nodes.filter(isAnOutputNode)).getOrElse(Seq()).distinct
    val result = layerNodes.filterNot(n => gbkFirst.exists(gbk => transitiveUses(gbk).contains(n))).
      filterNot(visited.contains)
    Layer(result.distinct)
  }

  private def transitiveUsesUntil(inputs: Seq[CompNode], until: CompNode => Boolean): Seq[CompNode] = {
    if (inputs.isEmpty) Seq()
    else {
      val (stop, continue) = inputs.flatMap(uses).toSeq.partition(until)
      stop ++ continue ++ transitiveUsesUntil(continue, until)
    }
  }

  def groupInputChannels(layer: Layer[T]): Seq[Seq[InputChannel]] = {
    Seqs.transitiveClosure(inputChannels(layer)) { (i1: InputChannel, i2: InputChannel) =>
      (i1.tags intersect i2.tags).nonEmpty
    }.map(_.list)
  }

  def inputChannels(layer: Layer[T]): Seq[InputChannel] = gbkInputChannels(layer) ++ floatingInputChannels(layer)

  def inputNodes(layer: Layer[T]): Seq[CompNode] =
    layer.nodes.filterNot(isValueNode).collect {
      case node if children(node).exists(!layer.nodes.contains(_)) => children(node).filterNot(n => layer.nodes.contains(n) || isValueNode(n))
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
    val gbkChannels = gbkInputChannels(layer)
    val gbks = gbkChannels.flatMap(_.groupByKeys)
    val inputs = inputNodes(layer)

    inputs.map { inputNode =>
      val mappers = transitiveUses(inputNode)
        .collect(isAParallelDo)
        .filter(layer.nodes.contains)
        .filterNot(gbkChannels.flatMap(_.mappers).contains)
        .filterNot(n => uses(n).nonEmpty && uses(n).forall(gbks.contains)).toSeq
      val lastLayer = layersOf(mappers, _ => true).lastOption.getOrElse(Layer()).nodes.collect(isAParallelDo)
      new FloatingInputChannel(inputNode, lastLayer, this)
    }.filterNot(_.isEmpty)
  }

  def gbkOutputChannels(layer: Layer[T]): Seq[OutputChannel] = {
    val gbks = layer.nodes.collect(isAGroupByKey)
    gbks.map(gbk => gbkOutputChannel(gbk, layer))
  }

  /**
   * @return a gbk output channel based on the nodes which are following the gbk
   */
  def gbkOutputChannel(gbk: GroupByKey, layer: Layer[T]): GbkOutputChannel = {
    parents(gbk) match {
      case (c: Combine) +: (p: ParallelDo) +: rest if isReducer(p) => GbkOutputChannel(gbk, combiner = Some(c), reducer = Some(p))
      case (p: ParallelDo) +: rest                 if isReducer(p) => GbkOutputChannel(gbk, reducer = Some(p))
      case (c: Combine) +: rest                                    => GbkOutputChannel(gbk, combiner = Some(c))
      case _                                                       => GbkOutputChannel(gbk)
    }
  }


  def outputChannels(layer: Layer[T]): Seq[OutputChannel] = gbkOutputChannels(layer) ++ floatingOutputChannels(layer)

  def floatingOutputChannels(layer: Layer[T]): Seq[OutputChannel] = {
    val floatingMappers = inputChannels(layer).flatMap(_.bypassOutputNodes)
    floatingMappers.distinct.map(BypassOutputChannel(_))
  }

  def selectNode: CompNode => Boolean = (n: CompNode) => true

  def isAnInputNode(nodes: Seq[CompNode]): CompNode => Boolean = (node: CompNode) =>
    !isValueNode(node) &&
      (children(node).isEmpty || children(node).forall(!nodes.contains(_)))

  def isAnOutputNode: CompNode => Boolean = (isMaterialised  || isGroupByKey || isEndNode || isCheckpoint) && !isReturn

  /** node at the end of the graph */
  def isEndNode: CompNode => Boolean = attr { n =>
    parent(n).isEmpty
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
    case pd @ ParallelDo1((cb @ Combine1((gbk: GroupByKey))) +: rest) => rest.isEmpty && isReturn(pd.env) && isUsedAtMostOnce(pd) && isUsedAtMostOnce(cb) && isUsedAtMostOnce(gbk)
    case pd @ ParallelDo1((gbk: GroupByKey) +: rest)                  => rest.isEmpty && isReturn(pd.env) && isUsedAtMostOnce(pd) && isUsedAtMostOnce(gbk)
    case _                                                            => false
  }

  lazy val isAReducer: CompNode => Boolean = attr {
    case node: ParallelDo if isReducer(node) => true
    case _                                   => false
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
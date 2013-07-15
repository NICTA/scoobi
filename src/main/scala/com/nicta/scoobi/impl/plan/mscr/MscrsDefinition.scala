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

/**
 * This trait processes the computation graph created out of DLists and creates map-reduce jobs from it.
 *
 * The algorithm consists in:
 *
 * - building layers of independent nodes in the graph
 * - finding the input nodes for the first layer
 * - reaching "output" nodes from the input nodes
 * - building output channels with those nodes
 * - building input channels connecting the output to the input nodes
 * - aggregating input and output channels as Mscr representing a full map reduce job
 * - iterating on any processing node that is not part of a Mscr
 */
trait MscrsDefinition extends Layering with Optimiser with ShowNode {

  /**
   * create layers of MapReduce jobs from the computation graph defined by the start node
   * where each layer contains independent map reduce jobs
   */
  def createMapReduceLayers(start: CompNode): Seq[Layer] =
    createLayers(Seq(start)).filterNot(_.isEmpty)

  /**
   * From start nodes in the graph and the list of already visited nodes, create new layers of MapReduce jobs
   */
  private def createLayers(startNodes: Seq[CompNode], visited: Seq[CompNode] = Seq()): Seq[Layer] =
    processLayers(startNodes.distinct, visited) match {
      case firstLayer +: rest => {
        val mscrLayer = createMscrs(inputNodes(firstLayer), visited)
        mscrLayer +: createLayers(mscrLayer.outputNodes, visited ++ firstLayer ++ mscrLayer.nodes)
      }
      case Nil => Nil
    }

  /** @return non-empty layers of processing nodes */
  private def processLayers(startNodes: Seq[CompNode], visited: Seq[CompNode]) =
    layersOf(startNodes.distinct).map(layer => layer.filterNot(isLoad || isValueNode || visited.contains)).filter(_.nonEmpty)

  /**
   * create a layer of Mscrs from input nodes, making sure not to use already visited nodes
   */
  private def createMscrs(inputNodes: Seq[CompNode], visited: Seq[CompNode]): Layer = {

    // get all the nodes accessible from the input nodes and leading to an output node
    val layer = createInputOutputLayer(inputNodes, visited)
    val outChannels = outputChannels(layer)
    val channelsWithCommonTags = groupInputChannels(layer)

    // create Mscr for each set of channels with common tags
    Layer(channelsWithCommonTags.map { inputChannels =>
      val correspondingOutputTags = inputChannels.flatMap(_.tags)
      val out = outChannels.filter(o => correspondingOutputTags.contains(o.tag))

      Mscr.create(inputChannels, out)
    }.filterNot(_.isEmpty))
  }

  /**
   * get all the nodes going from an input nodes to an output
   */
  private def createInputOutputLayer(inputNodes: Seq[CompNode], visited: Seq[CompNode]): Seq[CompNode] = {
    val layerNodes       = transitiveUsesUntil(inputNodes, isAnOutputNode)
    val outputs          = layerNodes.filter(isAnOutputNode).filterNot(visited.contains)
    val outputLayers     = layersOf(outputs, isAnOutputNode)
    val firstOutputLayer = outputLayers.dropWhile(l => !l.exists(outputs.contains)).headOption.map(_.filter(isAnOutputNode)).getOrElse(Seq()).distinct

    layerNodes.filterNot(n => firstOutputLayer.exists(gbk => transitiveUses(gbk).contains(n)))
      .filterNot(visited.contains)
      .distinct
  }

  private def transitiveUsesUntil(inputs: Seq[CompNode], until: CompNode => Boolean): Seq[CompNode] = {
    if (inputs.isEmpty) Seq()
    else {
      val (stop, continue) = inputs.flatMap(uses).toSeq.partition(until)
      stop ++ continue ++ transitiveUsesUntil(continue, until)
    }
  }

  def groupInputChannels(layer: Seq[CompNode]): Seq[Seq[InputChannel]] = {
    Seqs.transitiveClosure(inputChannels(layer)) { (i1: InputChannel, i2: InputChannel) =>
      (i1.tags intersect i2.tags).nonEmpty
    }.map(_.list)
  }

  def inputChannels(layer: Seq[CompNode]): Seq[InputChannel] = gbkInputChannels(layer) ++ floatingInputChannels(layer)

  def inputNodes(nodes: Seq[CompNode]): Seq[CompNode] =
    nodes.filterNot(isValueNode).collect {
      case node if children(node).exists(!nodes.contains(_)) => children(node).filterNot(n => nodes.contains(n) || isValueNode(n))
    }.distinct.flatten

  def gbkInputChannels(layer: Seq[CompNode]): Seq[GbkInputChannel] = {
    val gbks = layer.filter(isGroupByKey)
    val in = inputNodes(layer)
    in.flatMap { inputNode =>
      val groupByKeyUses = transitiveUses(inputNode).collect(isAGroupByKey).filter(gbks.contains).toSeq
      if (groupByKeyUses.isEmpty) Seq()
      else                        Seq(new GbkInputChannel(inputNode, groupByKeyUses, this))
    }
  }

  def floatingInputChannels(layer: Seq[CompNode]): Seq[FloatingInputChannel] = {
    val gbkChannels = gbkInputChannels(layer)
    val gbks = gbkChannels.flatMap(_.groupByKeys)
    val inputs = inputNodes(layer)

    inputs.map { inputNode =>
      val mappers = transitiveUses(inputNode)
        .collect(isAParallelDo)
        .filter(layer.contains)
        .filterNot(gbkChannels.flatMap(_.mappers).contains)
        .filterNot(n => uses(n).nonEmpty && uses(n).forall(gbks.contains)).toSeq
      val lastLayer = layersOf(mappers).lastOption.getOrElse(Seq()).collect(isAParallelDo)
      new FloatingInputChannel(inputNode, lastLayer, this)
    }.filterNot(_.isEmpty)
  }

  def gbkOutputChannels(layer: Seq[CompNode]): Seq[OutputChannel] = {
    val gbks = layer.collect(isAGroupByKey)
    gbks.map(gbk => gbkOutputChannel(gbk))
  }

  /**
   * @return a gbk output channel based on the nodes which are following the gbk
   */
  def gbkOutputChannel(gbk: GroupByKey): GbkOutputChannel = {
    parents(gbk) match {
      case (c: Combine) +: (p: ParallelDo) +: rest if isReducer(p) => GbkOutputChannel(gbk, combiner = Some(c), reducer = Some(p), nodes = this)
      case (p: ParallelDo) +: rest                 if isReducer(p) => GbkOutputChannel(gbk, reducer = Some(p), nodes = this)
      case (c: Combine) +: rest                                    => GbkOutputChannel(gbk, combiner = Some(c), nodes = this)
      case _                                                       => GbkOutputChannel(gbk, nodes = this)
    }
  }


  def outputChannels(layer: Seq[CompNode]): Seq[OutputChannel] = gbkOutputChannels(layer) ++ floatingOutputChannels(layer)

  def floatingOutputChannels(layer: Seq[CompNode]): Seq[OutputChannel] = {
    val floatingMappers = inputChannels(layer).flatMap(_.bypassOutputNodes)
    floatingMappers.distinct.map(m => BypassOutputChannel(m, nodes = this))
  }

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
}
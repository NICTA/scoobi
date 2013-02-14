/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package impl
package plan
package mscr

import scalaz.Scalaz._
import scalaz.syntax.std.indexedSeq._
import control.Functions._
import collection._
import comp._
import core._

trait MscrsDefinition extends Layering {

  def selectNode(n: CompNode) = isGroupByKey(n) || (n -> isFloating)

  /** a floating node is a parallelDo node that's not a descendent of a gbk node and is not a reducer */
  lazy val isFloating: CompNode => Boolean = attr("isFloating") {
    case pd: ParallelDo => (transitiveUses(pd).forall(!isGroupByKey) || uses(pd).exists(isMaterialise) || uses(pd).exists(isRoot) || !parent(pd).isDefined) &&
                            !isReducer(pd) &&
                           (!inputs(pd).exists(isFloating)) ||
                            descendents(pd.env).exists(isGroupByKey) ||
                            hasMaterialisedEnv(pd)
    case _              => false
  }

  private def hasMaterialisedEnv(pd: ParallelDo) =
    (pd.env +: descendentsUntil(isGroupByKey || isParallelDo)(pd.env)).exists(isMaterialise)

  /** all the mscrs for a given layer */
  lazy val mscrs: Layer[T] => Seq[Mscr] =
    attr("mscrs") { case layer => gbkMscrs(layer) ++ pdMscrs(layer) }

  /** Mscrs for parallel do nodes which are not part of a Gbk mscr */
  lazy val pdMscrs: Layer[T] => Seq[Mscr] = attr("parallelDo mscrs") { case layer =>
    val inputChannels = floatingParallelDos(layer).flatMap(pd => pd.ins.map(source => new FloatingInputChannel(source, floatingMappers(source))))
    val outputChannels = inputChannels.flatMap(_.lastMappers.map(BypassOutputChannel(_)))
    makeMscrs(inputChannels, outputChannels)
  }

  private [scoobi]
  def floatingMappers(sourceNode: CompNode) = {
    val floatings =
      if (isFloating(sourceNode)) Seq(sourceNode)
      else                        uses(sourceNode).filter(isFloating)

    (floatings ++ floatings.flatMap(mappersUses).filterNot(isFloating)).collect(isAParallelDo).toSeq
  }

  /** Mscrs for mscrs built around gbk nodes */
  lazy val gbkMscrs: Layer[T] => Seq[Mscr] = attr("gbk mscrs") { case layer =>
    makeMscrs(gbkInputChannels(layer), gbkOutputChannels(layer))
  }

  /**
   * make Mscrs by grouping input channels when their output go to the same output channel
   */
  private def makeMscrs(in: Seq[InputChannel], out: Seq[OutputChannel]): Seq[Mscr] = {
    if (out.isEmpty) Seq()
    else {
      // groups of input channels having at least one tag in common
      val channelsWithCommonTags = in.toList.groupWhen((i1, i2) => (i1.tags intersect i2.tags).nonEmpty)

      // create Mscr for each set of channels with common tags
      channelsWithCommonTags.map { taggedInputChannels =>
        val correspondingOutputTags = taggedInputChannels.flatMap(_.tags)
        val outputChannels = out.filter(o => correspondingOutputTags.contains(o.tag))
        Mscr.create(taggedInputChannels, (if (outputChannels.isEmpty) taggedInputChannels.map(_.sourceNode).collect { case pd: ParallelDo => BypassOutputChannel(pd) } else outputChannels))
      }
    }
  }

  /** create a gbk output channel for each gbk in the layer */
  lazy val gbkOutputChannels: Layer[T] => Seq[OutputChannel] = {
    attr("gbk output channels") { case layer =>
      layer.gbks.map(gbk => gbkOutputChannel(gbk))
    }
  }

  /**
\   * @return a gbk output channel based on the nodes which are following the gbk
   */
  protected def gbkOutputChannel(gbk: GroupByKey): GbkOutputChannel = {
    parents(gbk) match {
      case (c: Combine) +: (p: ParallelDo) +: rest if isReducer(p) => GbkOutputChannel(gbk, combiner = Some(c), reducer = Some(p))
      case (p: ParallelDo) +: rest                 if isReducer(p) => GbkOutputChannel(gbk, reducer = Some(p))
      case (c: Combine) +: rest                                    => GbkOutputChannel(gbk, combiner = Some(c))
      case _                                                       => GbkOutputChannel(gbk)
    }
  }

  lazy val gbkInputChannels: Layer[T] => Seq[MscrInputChannel] = attr("mscr input channels") { case layer =>
    layerGbkSourceNodes(layer).map { sourceNode =>
      val groupByKeyUses = transitiveUses(sourceNode).collect(isAGroupByKey).filter(layer.gbks.contains).toSeq
      new GbkInputChannel(sourceNode, groupByKeyUses)
    }
  }

  /**
   * flattened tree of mappers using this a given node
   */
  lazy val mappersUses: CompNode => Seq[ParallelDo] = attr { case node =>
    val (pds, _) = uses(node).partition(isParallelDo)
    pds.collect(isAParallelDo).toSeq ++ pds.filter { pd => (isParallelDo && !isFloating)(pd.parent[CompNode]) }.flatMap(mappersUses)
  }

  /** @return true if a ParallelDo node is not a leaf of a tree of mappers connected to a given source node */
  lazy val isInsideMapper: ParallelDo => Boolean = attr { case node =>
    uses(node).nonEmpty && uses(node).forall(isParallelDo && !isFloating)
  }

  lazy val layerInputs: Layer[T] => Seq[CompNode] = attr("layer inputs") { case layer =>
    layer.nodes.toSeq.flatMap(_ -> inputs).flatMap {       case other         => Seq(other)
    }.distinct
  }

  lazy val inputNodes: Mscr => Seq[CompNode] =
    attr("mscr source nodes") { case mscr => mscr.inputNodes }

  lazy val layerSourceNodes: Layer[T] => Seq[CompNode] = attr("layer source nodes") { case layer =>
    layer.nodes.flatMap(sourceNodes).distinct.filterNot(isValueNode)
  }
  lazy val layerGbkSourceNodes: Layer[T] => Seq[CompNode] = attr("layer source nodes") { case layer =>
    layer.gbks.flatMap(sourceNodes).distinct.filterNot(isValueNode)
  }
  lazy val sourceNodes: CompNode => Seq[CompNode] = attr("node sources") { case node =>
    val (sources, nonSources) = (node -> children).partition(isSourceNode)
    (sources ++ nonSources.filter(isParallelDo).flatMap(sourceNodes)).distinct
  }
  lazy val isSourceNode: CompNode => Boolean = attr("isSource") {
    case pd: ParallelDo => isReducer(pd)
    case other          => true
  }
  lazy val layerSinks: Layer[T] => Seq[Sink] =
    attr("layer sinks") { case layer => mscrs(layer).flatMap(_.sinks).distinct }

  lazy val layerSources: Layer[T] => Seq[Source] =
    attr("layer sources") { case layer => mscrs(layer).flatMap(_.sources).distinct }

  lazy val layerBridgeSinks: Layer[T] => Seq[Bridge] =
    attr("layer bridge sinks") { case layer => layerSinks(layer).collect { case bs: Bridge => bs } }

  lazy val layerBridgeSources: Layer[T] => Seq[Bridge] =
    attr("layer bridge sources") { case layer => layerSources(layer).collect { case bs: Bridge => bs } }

  /** collect all input nodes to the gbks of this layer */
  lazy val gbkInputs: Layer[T] => Seq[CompNode] = attr("gbk inputs") { case layer =>
    layer.nodes.flatMap(_ -> inputs).flatMap {
      case other          if layer.gbks.flatMap(_ -> inputs).contains(other) => Seq(other)
      case other                                                             => Seq()
    }.distinct
  }

  lazy val floatingParallelDos: Layer[T] => Seq[ParallelDo] = floatingNodes(isAParallelDo)

  def floatingNodes[N <: CompNode](pf: PartialFunction[CompNode, N]): Layer[T] => Seq[N] = attr("floating nodes") { case layer =>
    layer.nodes.collect(pf).filter(isFloating).toSeq
  }

  lazy val isReducer: ParallelDo => Boolean = attr("isReducer") {
    case pd @ ParallelDo1(Combine1((gbk: GroupByKey)) +: rest)    => rest.isEmpty && isUsedAtMostOnce(pd) && !hasMaterialisedEnv(pd)
    case pd @ ParallelDo1((gbk: GroupByKey) +: rest)              => rest.isEmpty && isUsedAtMostOnce(pd) && !hasMaterialisedEnv(pd)
    case pd if pd.bridgeStore.map(hasBeenFilled).getOrElse(false) => true
    case _                                                        => false
  }

}

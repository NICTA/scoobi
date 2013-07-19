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
package comp

import org.apache.commons.logging.LogFactory
import core._
import monitor.Loggable._
import org.kiama.rewriting._
import collection.+:
import control.Functions._
import scala.collection.mutable
import org.kiama.util.Emitter
import core.Emitter
import org.kiama.attribution.Attributable
import CollectFunctions._

/**
 * Optimiser for the CompNode graph
 *
 * It uses the [Kiama](http://code.google.com/p/kiama) rewriting library by defining Strategies for traversing the graph and rules to rewrite it.
 * Usually the rules are applied in a top-down fashion at every node where they can be applied (using the `everywhere` strategy).
 */
trait Optimiser extends CompNodes with MemoRewriter {
  implicit private lazy val logger = LogFactory.getLog("scoobi.Optimiser")

  /**
   * Combine nodes which are not the output of a GroupByKey must be transformed to a ParallelDo
   */
  lazy val combineToParDo = traverseSomebu(strategy {
    case c @ Combine(GroupByKey1(_),_,_,_,_,_) => None
    case c: Combine                            => Some(c.toParallelDo.debug(p => "combineToParDo "+c.id+" to "+p.id))
  })

  /**
   * Nested ParallelDos must be fused but only if pd1 is not used anywhere else.
   *
   * We use somebu to fuse the nodes "bottom-up" starting from all leaves of the tree at the same time
   *
   *    pd1 @ ParallelDo
   *          |
   *    pd2 @ ParallelDo
   *        ====>
   *    pd3 @ ParallelDo
   *
   * This rule is repeated until nothing can be fused anymore
   */
  def parDoFuse = traverseSomebu(parDoFuseRule)

  def parDoFuseRule = rule {
    case p2 @ ParallelDo((p1: ParallelDo) +: rest,_,_,_,_,_,_) if
      rest.isEmpty                          &&
      uses(p1).filterNot(_ == p2).isEmpty   &&
      !mustBeRead(p1)                       => ParallelDo.fuse(p1, p2).debug(p => "Fused "+p2.id+" with "+p1.id+". Result is "+p.id)
  }

  /** @return true if this parallelDo must be read ==> can't be fused */
  def mustBeRead(pd: ParallelDo): Boolean =
    hasBeenFilled(pd.bridgeStore) || pd.bridgeStore.isCheckpoint || pd.nodeSinks.nonEmpty

  def traverseOncebu(s: Strategy) = repeatTraversal(oncebu, s)
  def traverseSomebu(s: Strategy) = repeatTraversal(somebu, s)
  def traverseSometd(s: Strategy) = repeatTraversal(sometd, s)

  /**
   * apply a traversal strategy but make sure that:
   *
   * - after each pass the tree is reset in terms of attributable relationships and uses
   * - the strategy to execute is memoised, i.e. if a node has already been processed its result must be reused
   *   this ensures that rewritten shared nodes are not duplicated
   */
  def repeatTraversal(traversal: (String, Strategy) => Strategy, s: Strategy) = {
    repeat(resetTree(traversal(s.name, s)))
  }

  private def resetTree(s: =>Strategy): Strategy =  new Strategy("resetTree") {
    val body = (t1 : Any) => {
      t1 match {
        case c: CompNode => reinit(c)
        case _           => ()
      }
      s(t1)
    }
  }

  /**
   * add a map to output values to non-filled sink nodes if there are some
   */
  def addParallelDoForNonFilledSinks = oncebu(rule {
    case p: ProcessNode if p.sinks.exists(!hasBeenFilled) && p.sinks.exists(hasBeenFilled) =>
      logger.debug("add a parallelDo node to output non-filled sinks of "+p)
      ParallelDo.create(p)(p.wf).copy(nodeSinks = p.sinks.filterNot(hasBeenFilled))
  })

  /**
   * all the strategies to apply, in sequence
   */
  def allStrategies =
    combineToParDo                          <*
    parDoFuse                               <*
    attempt(addParallelDoForNonFilledSinks)

  /**
   * Optimise a set of CompNodes, starting from the set of outputs
   */
  def optimise(outputs: Seq[CompNode]): Seq[CompNode] =
    rewrite(allStrategies)(Root(outputs)) match {
      case Root1(ins) => ins
    }

  /** duplicate the whole graph by copying all nodes */
  lazy val duplicate = (node: CompNode) => rewrite(everywhere(rule {
    case n: Op          => n.copy()
    case n: Materialise => n.copy()
    case n: GroupByKey  => n.copy()
    case n: Combine     => n.copy()
    case n: ParallelDo  => n.copy()
    case n: Load        => n.copy()
    case n: Return      => n.copy()
  }))(node)

  /** apply one strategy to a list of Nodes. Used for testing */
  private[scoobi]
  def optimise(strategy: Strategy, nodes: CompNode*): List[CompNode] = {
    rewrite(strategy)(Root(nodes)) match {
      case Root1(ins) => ins.toList
    }
  }

  /**
   * optimise just one node which is the output of a graph.
   */
  private[scoobi]
  def optimise(node: CompNode): CompNode = {
    reinit(optimise(Seq(reinit(node))).headOption.getOrElse(node))
  }

  /** remove nodes from the tree based on a predicate */
  def truncate(node: CompNode)(condition: Any => Boolean) = {
    def isParentMaterialise(n: CompNode) = parent(n).exists(isMaterialise)
    def truncateNode(n: Any): Any =
      n match {
        case p: ParallelDo if isParentMaterialise(p) => p.copy(ins = Seq())
        case g: GroupByKey if isParentMaterialise(g) => g.copy(in = Return.unit)
        case c: Combine    if isParentMaterialise(c) => c.copy(in = Return.unit)
        case p: ProcessNode                          => Load(p.bridgeStore, p.wf)
        case other                                   => other
      }

    val truncateRule = rule { case n: Any =>
      if (condition(n)) truncateNode(n)
      else              n
    }
    reinit(rewrite(topdown(truncateRule))(node))
  }

  def truncateAlreadyExecutedNodes(node: CompNode)(implicit sc: ScoobiConfiguration) = {
    allSinks(node).collect { case ss: SinkSource if ss.checkpointExists(sc) => ss }.foreach(markSinkAsFilled)
    truncate(node) {
      case process: ProcessNode => nodeHasBeenFilled(process)
      case other                => false
    }
  }

}
object Optimiser extends Optimiser
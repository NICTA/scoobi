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
import org.kiama.rewriting.Rewriter
import collection.+:
import control.Functions._

/**
 * Optimiser for the CompNode graph
 *
 * It uses the [Kiama](http://code.google.com/p/kiama) rewriting library by defining Strategies for traversing the graph and rules to rewrite it.
 * Usually the rules are applied in a top-down fashion at every node where they can be applied (using the `everywhere` strategy).
 */
trait Optimiser extends CompNodes with Rewriter {
  implicit private lazy val logger = LogFactory.getLog("scoobi.Optimiser")

  /**
   * Combine nodes which are not the output of a GroupByKey must be transformed to a ParallelDo
   */
  def combineToParDo = everywhere(rule {
    case c @ Combine(GroupByKey1(_),_,_,_,_,_) => c
    case c: Combine                            => c.debug("combineToParDo").toParallelDo
  })

  /**
   * Nested ParallelDos must be fused but only if pd1 is not used anywhere else
   *
   *    pd1 @ ParallelDo
   *          |
   *    pd2 @ ParallelDo
   *        ====>
   *    pd3 @ ParallelDo
   *
   * This rule is repeated until nothing can be fused anymore
   */
  def parDoFuse = repeat(oncebu(rule {
    case p2 @ ParallelDo((p1 @ ParallelDo1(_)) +: rest,_,_,_,_,_,_) if
      uses(p1).filterNot(_ == p2).isEmpty                 &&
      rest.isEmpty                                        &&
      !p1.bridgeStore.map(hasBeenFilled).getOrElse(false) &&
      p1.nodeSinks.isEmpty                                   => ParallelDo.fuse(p1.debug("parDoFuse with "+p2), p2)
  }))

  /**
   * add a bridgeStore if it is necessary to materialise a value and no bridge is available
   */
  def addBridgeStore = everywhere(rule {
    case m @ Materialise1(p: ProcessNode) if !p.bridgeStore.isDefined => m.copy(p.addSink(p.createBridgeStore)).debug("add bridgestore to "+p)
  })

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
  def allStrategies(outputs: Seq[CompNode]) =
    attempt(combineToParDo)                 <*
    attempt(parDoFuse     )                 <*
    attempt(addBridgeStore)                 <*
    attempt(addParallelDoForNonFilledSinks)

  /**
   * Optimise a set of CompNodes, starting from the set of outputs
   */
  def optimise(outputs: Seq[CompNode]): Seq[CompNode] =
    rewrite(allStrategies(outputs))(outputs)

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
    rewrite(strategy)(nodes).toList
  }

  /**
   * optimise just one node which is the output of a graph.
   *
   * It is very important to duplicate the whole graph first to avoid execution information to become attached to the original
   * nodes. Because if the main graph is augmented, the execution information we want to retrieve (like which nodes are using
   * another node as an environment) may change.
   */
  private[scoobi]
  def optimise(node: CompNode): CompNode = {
    reinitUses
    val result = reinitAttributable(optimise(Seq(reinitAttributable(node))).headOption.getOrElse(node))
    reinitUses
    result
  }

  /** remove nodes from the tree based on a predicate */
  def truncate(node: CompNode)(condition: Term => Boolean) = {
    def truncateNode(n: Term): Term =
      n match {
        case pd: ParallelDo   => pd.copy(ins = Seq(Return.unit))
        case cb: Combine      => cb.copy(in = Return.unit)
        case gbk: GroupByKey  => gbk.copy(in = Return.unit)
        case other            => other
      }

    val truncateRule = rule { case n: Term =>
      if (condition(n)) truncateNode(n)
      else              n
    }
    val result = reinitAttributable(rewrite(topdown(truncateRule))(node))
    reinitUses
    result
  }

}
object Optimiser extends Optimiser
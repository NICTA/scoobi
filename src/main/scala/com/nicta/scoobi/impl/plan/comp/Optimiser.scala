package com.nicta.scoobi
package impl
package plan
package comp

import org.apache.commons.logging.LogFactory
import core._
import monitor.Loggable._
import org.kiama.rewriting.Rewriter
import collection.+:

/**
 * Optimiser for the DComp AST graph
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
    case c @ Combine(GroupByKey1(_),_,_,_,_) => c
    case c: Combine[_,_]                     => c.debug("combineToParDo").toParallelDo
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
  def parDoFuse(pass: Int) = repeat(sometd(rule {
    case p2 @ ParallelDo((p1 @ ParallelDo1(_)) +: rest,_,_,_,_,_) if uses(p1).isEmpty && rest.isEmpty && !hasBeenExecuted(p1) && !hasBeenExecuted(p2) =>
      p1.debug("parDoFuse (pass "+pass+") ").fuse(p2)(p2.mwf.asInstanceOf[ManifestWireFormat[Any]], p2.mwfe)
  }))

  /**
   * all the strategies to apply, in sequence
   */
  def allStrategies(outputs: Seq[CompNode]) =
    attempt(parDoFuse(pass = 1)      ) <*
    attempt(combineToParDo           ) <*
    attempt(parDoFuse(pass = 2)      )

  /**
   * Optimise a set of CompNodes, starting from the set of outputs
   */
  def optimise(outputs: Seq[CompNode]): Seq[CompNode] =
    rewrite(allStrategies(outputs))(outputs)

  /** duplicate the whole graph by copying all nodes */
  lazy val duplicate = (node: CompNode) => rewrite(everywhere(rule {
    case n: Op[_,_,_]         => updateCopy(n, n.copy())
    case n: Materialize[_]    => updateCopy(n, n.copy())
    case n: GroupByKey[_,_]   => updateCopy(n, n.copy())
    case n: Combine[_,_]      => updateCopy(n, n.copy())
    case n: ParallelDo[_,_,_] => updateCopy(n, n.copy())
    case n: Load[_]           => updateCopy(n, n.copy())
    case n: Return[_]         => updateCopy(n, n.copy())
  }))(node)

  /** mark the original node as 'optimised' and return the copy of the node */
  private def updateCopy(original: CompNode, copy: CompNode) = {
    if (executed.hasBeenComputedAt(original))
      executed(copy)
    copy
  }

  override protected def dup[T <: Product] (t : T, children : Array[AnyRef]) : T = {
    t match {
      case original: CompNode => {
        val duplicated = super.dup(t, children)
        updateCopy(original, duplicated.asInstanceOf[CompNode])
        duplicated
      }
      case other => super.dup(t, children)
    }
  }

  /**
   * simple attribute to mark a node as executed
   * By calling executed.hasBeenComputed(node) we can know if a node has been optimised or not, hence executed afterwards
   */
  protected lazy val executed: CachedAttribute[CompNode, CompNode] = attr("executed") { case n: CompNode => n }

  /** @return true if a node has been executed */
  protected def hasBeenExecuted(n: CompNode) = executed.hasBeenComputedAt(n)

  /** apply one strategy to a list of Nodes. Used for testing */
  private[scoobi]
  def optimise(strategy: Strategy, nodes: CompNode*): List[CompNode] = {
    rewrite(strategy)(nodes).toList
  }

  /**
   * optimise just one node which is the output of a graph.
   *
   * It is very import to duplicate the whole graph first to avoid execution information to become attached to the original
   * nodes. Because if the main graph is augmented, the execution information we want to retrieve (like which nodes are using
   * another node as an environment) may change.
   */
  private[scoobi]
  def optimise(node: CompNode): CompNode =
    optimise(Seq(duplicate(node))).headOption.getOrElse(node)
}
object Optimiser extends Optimiser
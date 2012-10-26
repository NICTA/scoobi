package com.nicta.scoobi
package impl
package plan
package comp

import org.kiama.rewriting.Rewriter._
import core._

/**
 * Optimiser for the DComp AST graph
 *
 * It uses the [Kiama](http://code.google.com/p/kiama) rewriting library by defining Strategies for traversing the graph and rules to rewrite it.
 * Usually the rules are applied in a top-down fashion at every node where they can be applied (using the `everywhere` strategy).
 */
trait Optimiser extends CompNodes {

  /**
   * Flatten nodes which are input to several other nodes must be duplicated
   *        Flatten1
   *        /     \
   *     node1   node2
   *         ====>
   *    Flatten1  Flatten2
   *     |           |
   *    node1      node2
   */
  def flattenSplit = everywhere(rule {
    case f : Flatten[_] => f.copy()
  })

  /**
   * Flatten nodes which are input to a Parallel do must be transformed to the ParallelDo being replicated in each input
   * of the Flatten node
   *
   *    in1  in2  in3
   *      \   |   /
   *       Flatten
   *          |
   *    pd @ ParallelDo
   *        ====>
   *    in1  in2  in3
   *     |    |    |
   *     pd  pd   pd
   *      \   |   /
   *       Flatten
   */
  def flattenSink = everywhere(rule {
    case p @ ParallelDo(fl @ Flatten1(ins),_,_,_,_,_) => fl.copy(ins = fl.ins.map(i => p.copy(in = i)))
  })

  /**
   * Nested Flattens must be fused
   *
   *    in1  in2     in3   in4
   *     \   /         \  /
   *   Flatten1 node1  Flatten2
   *         \    |    /
   *          Flatten
   *           ====>
   *     in1 in2 node1 in3 in4
   *       \  \    |   /   /
   *          Flatten
   *
   * This rule is repeated until nothing can be flattened anymore
   */
  def flattenFuse = repeat(sometd(rule {
    case fl @ Flatten1(ins) if ins exists isFlatten => fl.copy(ins = fl.ins.flatMap { case Flatten1(nodes) => nodes; case other => List(other) })
  }))

  /**
   * Combine nodes which are not the output of a GroupByKey must be transformed to a ParallelDo
   */
  def combineToParDo = everywhere(rule {
    case c @ Combine(GroupByKey1(_),_,_,_) => c
    case c: Combine[_,_]                   => c.toParallelDo
  })

  /**
   * Nested ParallelDos must be fused
   *
   *    pd1 @ ParallelDo
   *          |
   *    pd2 @ ParallelDo
   *        ====>
   *    pd3 @ ParallelDo
   *
   * This rule is repeated until nothing can be flattened anymore
   */
  def parDoFuse = repeat(sometd(rule {
    case p1 @ ParallelDo(p2: ParallelDo[_,_,_],_,_,_,_,Barriers(_,false)) => p2.fuse(p1)(p1.mwf.asInstanceOf[ManifestWireFormat[Any]], p1.mwfe)
  }))

  /**
   * A GroupByKey which is an input to several nodes must be copied.
   *
   *       GroupByKey
   *        /     \
   *     node1   node2
   *         ====>
   *  GroupByKey1  GroupByKey2
   *     |             |
   *    node1        node2
   */
  def groupByKeySplit = everywhere(rule {
    // I think that this case is redundant with the flattenSplit rule
    case g @ GroupByKey1(f: Flatten[_]) => g.copy(in = f.copy())
    case g: GroupByKey[_,_]             => g.copy()
  })

  /**
   * A Combine which is an input to several nodes must be copied.
   *
   *        Combine
   *        /     \
   *     node1   node2
   *         ====>
   *   Combine1  Combine2
   *     |          |
   *    node1     node2
   */
  def combineSplit = everywhere(rule {
    case c: Combine[_,_] => c.copy()
  })

  /**
   * A ParallelDo which is in the list of outputs must be marked with a fuseBarrier
   */
  def parDoFuseBarrier(outputs: Set[CompNode]) = everywhere(rule {
    case p: ParallelDo[_,_,_] if outputs contains p => p.copy(barriers = p.barriers.copy(fuseBarrier = true))
  })

  /**
   * all the strategies to apply, in sequence
   */
  def allStrategies(outputs: Set[CompNode]) =
    attempt(flattenSplit     ) <*
    attempt(flattenSink      ) <*
    attempt(flattenFuse      ) <*
    attempt(combineToParDo   ) <*
    attempt(parDoFuse        ) <*
    attempt(groupByKeySplit  ) <*
    attempt(combineSplit     ) <*
    attempt(parDoFuseBarrier(outputs))

  /**
   * Optimise a set of CompNodes, starting from the set of outputs
   */
  def optimise(outputs: Set[CompNode]): Set[CompNode] =
    rewrite(allStrategies(outputs))(outputs)

  /** duplicate the whole graph by copying all nodes */
  lazy val duplicate = (node: CompNode) => rewrite(everywhere(rule {
    case n: Op[_,_,_]         => n.copy()
    case n: Flatten[_]        => n.copy()
    case n: Materialize[_]    => n.copy()
    case n: GroupByKey[_,_]   => n.copy()
    case n: Combine[_,_]      => n.copy()
    case n: ParallelDo[_,_,_] => n.copy()
    case n: Load[_]           => n.copy()
    case n: Return[_]         => n.copy()
  }))(node)

  /** apply one strategy to a list of Nodes. Used for testing */
  private[scoobi] def optimise(strategy: Strategy, nodes: CompNode*): List[CompNode] = {
    rewrite(strategy)(nodes).toList
  }

  /** optimise just one node which is the output of a graph. Used for testing */
  private[scoobi] def optimise(node: CompNode): CompNode = optimise(Set(node)).headOption.getOrElse(node)
}
object Optimiser extends Optimiser
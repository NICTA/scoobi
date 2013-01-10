package com.nicta.scoobi
package impl
package plan
package comp

import core._
import control.Functions._
/**
 * General methods for navigating a graph of CompNodes
 */
trait CompNodes extends GraphNodes with CollectFunctions {
  type T = CompNode

  /**
   * compute the inputs of a given node
   * For a ParallelDo node this does not consider its environment
   */
  lazy val inputs : CompNode => Seq[CompNode] = attr("inputs") {
    // for a parallel do node just consider the input node, not the environment
    case pd: ParallelDo[_,_,_]  => pd.ins
    case n                      => incomings(n)
  }

  /** compute the incoming data of a given node: all the children of a node, including its environment for a parallelDo */
  lazy val incomings : CompNode => Seq[CompNode] =
    attr("incomings") { case n => children(n) }

  /** compute all the nodes which use a given node as an environment */
  def usesAsEnvironment : CompNode => Seq[ParallelDo[_,_,_]] = attr("usesAsEnvironment") { case node =>
    uses(node).collect { case pd: ParallelDo[_,_,_] if pd.env == node => pd }.toSeq
  }

}
object CompNodes extends CompNodes

/**
 * This functions can be used to filter or collect specific nodes in collections
 */
trait CollectFunctions {
  /** return true if a CompNode is a ParallelDo */
  lazy val isParallelDo: CompNode => Boolean = { case p: ParallelDo[_,_,_] => true; case other => false }
  /** return true if a CompNode is a Load */
  lazy val isLoad: CompNode => Boolean = { case l: Load[_] => true; case other => false }
  /** return true if a CompNode is a Load */
  lazy val isALoad: PartialFunction[CompNode, Load[_]] = { case l: Load[_] => l }
  /** return true if a CompNode is a Combine */
  lazy val isACombine: PartialFunction[Any, Combine[_,_]] = { case c: Combine[_,_] => c }
  /** return true if a CompNode is a ParallelDo */
  lazy val isAParallelDo: PartialFunction[Any, ParallelDo[_,_,_]] = { case p: ParallelDo[_,_,_] => p }
  /** return true if a CompNode is a GroupByKey */
  lazy val isGroupByKey: CompNode => Boolean = { case g: GroupByKey[_,_] => true; case other => false }
  /** return true if a CompNode is a GroupByKey */
  lazy val isAGroupByKey: PartialFunction[Any, GroupByKey[_,_]] = { case gbk: GroupByKey[_,_] => gbk }
  /** return true if a CompNode is a Materialise */
  lazy val isMaterialise: CompNode => Boolean = { case m: Materialise[_] => true; case other => false }
  /** return true if a CompNode is a Return */
  lazy val isReturn: CompNode => Boolean = { case r: Return[_]=> true; case other => false }
  /** return true if a CompNode needs to be persisted */
  lazy val isSinkNode: CompNode => Boolean = isMaterialise
  /** return true if a CompNode needs to be loaded */
  lazy val isValueNode: CompNode => Boolean = isReturn || isComputedValueNode
  /** return true if a CompNode needs to be computed */
  lazy val isComputedValueNode: CompNode => Boolean = isMaterialise || isOp
  /** return true if a CompNode is an Op */
  lazy val isOp: CompNode => Boolean = { case o: Op[_,_,_] => true; case other => false }
}
object CollectFunctions extends CollectFunctions
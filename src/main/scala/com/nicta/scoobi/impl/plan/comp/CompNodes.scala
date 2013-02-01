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
    case pd: ParallelDo => pd.ins
    case n              => children(n)
  }

  /** compute all the nodes which use a given node as an environment */
  def usesAsEnvironment : CompNode => Seq[ParallelDo] = attr("usesAsEnvironment") { case node =>
    uses(node).collect { case pd: ParallelDo if pd.env == node => pd }.toSeq
  }

  /** mark a bridge as filled so it doesn't have to be recomputed */
  protected def markBridgeAsFilled = (b: Bridge) => filledBridge(b.bridgeStoreId)
  /** this attribute stores the fact that a Bridge has received data */
  protected lazy val filledBridge: CachedAttribute[String, String] = attr("filled bridge")(identity)
  /** @return true if a given Bridge has already received data */
  protected def hasBeenFilled(b: Bridge)= filledBridge.hasBeenComputedAt(b.bridgeStoreId)
}
object CompNodes extends CompNodes

/**
 * This functions can be used to filter or collect specific nodes in collections
 */
trait CollectFunctions {
  /** return true if a CompNode is a ParallelDo */
  lazy val isParallelDo: CompNode => Boolean = { case p: ParallelDo => true; case other => false }
  /** return true if a CompNode is a Load */
  lazy val isLoad: CompNode => Boolean = { case l: Load => true; case other => false }
  /** return true if a CompNode is a Load */
  lazy val isALoad: PartialFunction[CompNode, Load] = { case l: Load => l }
  /** return true if a CompNode is a Combine */
  lazy val isACombine: PartialFunction[Any, Combine] = { case c: Combine => c }
  /** return true if a CompNode is a ParallelDo */
  lazy val isAParallelDo: PartialFunction[Any, ParallelDo] = { case p: ParallelDo => p }
  /** return true if a CompNode is a GroupByKey */
  lazy val isGroupByKey: CompNode => Boolean = { case g: GroupByKey => true; case other => false }
  /** return true if a CompNode is a GroupByKey */
  lazy val isAGroupByKey: PartialFunction[Any, GroupByKey] = { case gbk: GroupByKey => gbk }
  /** return true if a CompNode is a Materialise */
  lazy val isMaterialise: CompNode => Boolean = { case m: Materialise => true; case other => false }
  /** return the node if a CompNode is a Materialise */
  lazy val isAMaterialise: PartialFunction[Any, Materialise] = { case m: Materialise => m }
  /** return true if a CompNode is a Root */
  lazy val isRoot: CompNode => Boolean = { case r: Root => true; case other => false }
  /** return true if a CompNode is a Return */
  lazy val isReturn: CompNode => Boolean = { case r: Return=> true; case other => false }
  /** return true if a CompNode needs to be persisted */
  lazy val isSinkNode: CompNode => Boolean = isMaterialise
  /** return true if a CompNode needs to be loaded */
  lazy val isValueNode: CompNode => Boolean = isReturn || isComputedValueNode
  /** return true if a CompNode needs to be computed */
  lazy val isComputedValueNode: CompNode => Boolean = isMaterialise || isOp
  /** return true if a CompNode is an Op */
  lazy val isOp: CompNode => Boolean = { case o: Op => true; case other => false }
}
object CollectFunctions extends CollectFunctions
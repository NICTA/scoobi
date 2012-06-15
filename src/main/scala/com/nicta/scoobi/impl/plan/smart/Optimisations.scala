package com.nicta.scoobi
package impl
package plan
package smart

object Optimisations {
//
//  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//  //  Optimisation strategies:
//  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//  /** An optimisation strategy that any ParallelDo that is connected to an output is marked
//   * with a fuse barrier. */
//  def optAddFuseBar[A, Sh](copied: CopyTable, outputs: Set[DComp[_, _ <: Shape]]): (DComp[A, Sh], CopyTable, Boolean) =
//    copyOnceWith(copied, _.optAddFuseBar(_, outputs))
//
//  /** An optimisation strategy that replicates any Flatten nodes that have multiple outputs
//   * such that in the resulting AST Flatten nodes only have single outputs. */
//  def optSplitFlattens[A, Sh](copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
//    copyOnceWith(copied, _.optSplitFlattens(_))
//
//  /** An optimisation strategy that sinks a Flatten node that is an input to a ParallelDo node
//   * such that in the resulting AST replicated ParallelDo nodes are inputs to the Flatten
//   * node. */
//  def optSinkFlattens[A, Sh](copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
//    copyOnceWith(copied, _.optSinkFlattens(_))
//
//  /** An optimisation strategy that fuse any Flatten nodes that are inputs to Flatten nodes. */
//  def optFuseFlattens[A, Sh](copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
//    copyOnceWith(copied, _.optFuseFlattens(_))
//
//  /** An optimisation strategy that morphs a Combine node into a ParallelDo node if it does not
//   * follow a GroupByKey node. */
//  def optCombinerToParDos[A, Sh](copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
//    copyOnceWith(copied, _.optCombinerToParDos(_))
//
//  /** An optimisation strategy that fuses the functionality of a ParallelDo node that is an input
//   * to another ParallelDo node. */
//  def optFuseParDos[A, Sh](copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
//    copyOnceWith(copied, _.optFuseParDos(_))
//
//  /** An optimisation strategy that replicates any GroupByKey nodes that have multiple outputs
//   * such that in the resulting AST, GroupByKey nodes have only single outputs. */
//  def optSplitGbks[A, Sh](copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
//    copyOnceWith(copied, _.optSplitGbks(_))
//
//  /** An optimisation strategy that replicates any Combine nodes that have multiple outputs
//   * such that in the resulting AST, Combine nodes have only single outputs. */
//  def optSplitCombines[A, Sh](copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
//    copyOnceWith(copied, _.optSplitCombines(_))
//
//
//  /** Perform a depth-first traversal copy of the DComp node. When copying the input DComp
//   * node(s), a the CopyFn function is used (somewhat like a callback). */
//  def justCopy[A, Sh](copied: CopyTable, cf: CopyFn[_,_]): DComp[A, Sh] => (DComp[A, Sh], CopyTable, Boolean) =
//    (node: DComp[A, Sh]) => node match {
//      case Combine(in, f) => {
//        val cfKIV = cf.asInstanceOf[CopyFn[A, Arr]]
//        val (inUpd, copiedUpd, b) = cfKIV(in, copied)
//        val comb = Combine(inUpd, f)
//        (comb, copiedUpd + (this -> comb), b)
//      }
//      case Flatten(ins) => {
//        val (insUpd, copiedUpd, b) = justCopyInputs(ins, copied, cf.asInstanceOf[CopyFn[A, Arr]])
//        val flat = Flatten(insUpd)
//        (flat, copiedUpd + (this -> flat), b)
//      }
//      case GroupByKey(in) => {
//        val cfKV = cf.asInstanceOf[CopyFn[A, Arr]]
//        val (inUpd, copiedUpd, b) = cfKV(in, copied)
//        val gbk = GroupByKey(inUpd)
//        (gbk, copiedUpd + (this -> gbk), b)
//      }
//      case Materialize(in) => {
//        val cfA = cf.asInstanceOf[CopyFn[A, Arr]]
//        val (inUpd, copiedUpd, b) = cfA(in, copied)
//        val mat = Materialize(inUpd)
//        (mat, copiedUpd + (this -> mat), b)
//      }
//      case Op(in1, in2, f) => {
//        val cfA = cf.asInstanceOf[CopyFn[A, Exp]]
//        val (inUpd1, copiedUpd1, b1) = cfA(in1, copied)
//        val cfB = cf.asInstanceOf[CopyFn[B, Exp]]
//        val (inUpd2, copiedUpd2, b2) = cfB(in2, copiedUpd1)
//        val op = Op(inUpd1, inUpd2, f)
//        (op, copiedUpd2 + (this -> op), b1 || b2)
//      }
//      case ParallelDo(in, evn, dofn, groupBarrier, fuseBarrier) =>  {
//        val cfA = cf.asInstanceOf[CopyFn[A, Arr]]
//        val (inUpd, copiedUpd1, b1) = cfA(in, copied)
//
//        val cfE = cf.asInstanceOf[CopyFn[E, Exp]]
//        val (envUpd, copiedUpd2, b2) = cfE(env, copiedUpd1)
//
//        val pd = ParallelDo(inUpd, envUpd, dofn, groupBarrier, fb)
//        (pd, copiedUpd2 + (this -> pd), b1 || b2)
//      }
//      case other =>  (other, copied, false)
//    }
//
//  /** A helper method that checks whether the node has already been copied, and if so returns the copy, else
//   * invokes a user provided code implementing the copy. */
//  protected def copyOnce[A, Sh](copied: CopyTable)(newCopy: => (DComp[A, Sh], CopyTable, Boolean)): (DComp[A, Sh], CopyTable, Boolean) =
//    copied.get(this) match {
//      case Some(copy) => (copy.asInstanceOf[DComp[A, Sh]], copied, false)
//      case None       => newCopy
//    }
//
//  /* Helper for performing optimisation with depth-first traversal once-off copying. */
//  private def copyOnceWith[A, Sh](copied: CopyTable, cf: CopyFn[A, Sh]): (DComp[A, Sh], CopyTable, Boolean) =
//    copyOnce(copied) { justCopy(copied, cf) }


}

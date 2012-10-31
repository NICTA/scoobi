package com.nicta.scoobi
package impl
package plan
package comp

import core.CompNode
import mscr.MscrAttributes

trait factory extends CompNodeFactory with MscrAttributes {
  override def load                                   = init(super.load)
  override def flatten[A](nodes: CompNode*)           = init(super.flatten(nodes:_*))
  override def parallelDo(in: CompNode)               = init(super.parallelDo(in))
  override def rt                                     = init(super.rt)
  override def cb(in: CompNode)                       = init(super.cb(in))
  override def gbk(in: CompNode)                      = init(super.gbk(in))
  override def mt(in: CompNode)                       = init(super.mt(in))
  override def op(in1: CompNode, in2: CompNode)       = init(super.op(in1, in2))
  override def pd(in: CompNode, env: CompNode = rt, groupBarrier: Boolean = false, fuseBarrier: Boolean = false) =
    init(super.pd(in, env, groupBarrier, fuseBarrier))

  /** show before and after the optimisation */
  def optimisation(node: CompNode, optimised: CompNode) =
    if (show(node) != show(optimised)) "INITIAL: \n"+show(node)+"\nOPTIMISED:\n"+show(optimised) else "no optimisation"

  def show(node: CompNode): String =
    "SHOWING NODE: "+showNode(node, None)+"\n"+mscrsGraph(init(ancestors(node).headOption.getOrElse(node)))
}
import mscr.{MscrMaker, MscrAttributes}
trait graph extends MscrMaker


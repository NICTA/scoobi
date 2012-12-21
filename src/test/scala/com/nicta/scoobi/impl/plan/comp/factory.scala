package com.nicta.scoobi
package impl
package plan
package comp

import core.CompNode
import mscr.MscrAttributes
import org.kiama.rewriting.Rewriter

trait factory extends nodesFactory with MscrAttributes with ShowNodeMscr with Optimiser {
  override def show(node: CompNode): String =
    "SHOWING NODE: "+showNode(node, None)
}

trait nodesFactory extends CompNodeFactory with CompNodes with ShowNode {
  override def load                                                 = init(super.load)
  override def aRoot(nodes: CompNode*)                              = init(super.aRoot(nodes:_*))
  override def rt                                                   = init(super.rt)
  override def cb(in: CompNode)                                     = init(super.cb(in))
  override def gbk(in: CompNode)                                    = init(super.gbk(in))
  override def mt(in: CompNode)                                     = init(super.mt(in))
  override def op(in1: CompNode, in2: CompNode)                     = init(super.op(in1, in2))
  override def parallelDo(in: CompNode)                             = init(super.parallelDo(in))
  override def pd(ins: CompNode*): ParallelDo[String,String,String] = init(super.pd(ins:_*))
  override def pd(in: CompNode, env: CompNode): ParallelDo[String,String,String] = init(super.pd(in, env))

  /** show before and after the optimisation */
  def optimisation(node: CompNode, optimised: CompNode) =
  if (show(node) != show(optimised)) "INITIAL: \n"+show(node)+"\nOPTIMISED:\n"+show(optimised) else "no optimisation"

  def show(node: CompNode): String = "SHOWING NODE: "+showNode(node, None)

  /** initialize the Kiama attributes of a CompNode */
  def init[T <: CompNode](t: T): T  = initAttributable(t)

  val rewriter = new Rewriter {}

  def collectCombine          = rewriter.collectl { case c @ Combine1(_) => c: CompNode }
  def collectCombineGbk       = rewriter.collectl { case c @ Combine(GroupByKey1(_),_,_,_,_) => c }
  def collectParallelDo       = rewriter.collectl { case p: ParallelDo[_,_,_] => p }
  def collectSuccessiveParDos = rewriter.collectl { case p @ ParallelDo(ParallelDo1(_),_,_,_,_,_) => p }
  def collectGroupByKey       = rewriter.collectl { case g @ GroupByKey1(_) => g }

}
object factory extends factory

import mscr.MscrAttributes
trait graph


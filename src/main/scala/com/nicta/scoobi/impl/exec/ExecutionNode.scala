package com.nicta.scoobi
package impl
package exec

import plan.comp.CompNode

/**
 * GADT representing elementary computations to perform in hadoop jobs
 */
sealed trait ExecutionNode extends CompNode {
  def comp: CompNode
}

case class LoadExec(comp: CompNode) extends ExecutionNode
case class ReturnExec(comp: CompNode) extends ExecutionNode
case class FlattenExec(comp: CompNode, ins: Seq[CompNode]) extends ExecutionNode

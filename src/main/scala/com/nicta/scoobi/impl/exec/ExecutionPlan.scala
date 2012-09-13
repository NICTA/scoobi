package com.nicta.scoobi
package impl
package exec

import plan._
import comp.{CompNode, DComp}
import plan.graph.Mscr
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Rewriter

/**
 * The execution transforms the DComp nodes as created by the user and the Mscrs computed by the MscrGraph
 * and creates a graph of ExecutionNodes which will map to the final MSCRS to be executed by Hadoop.
 *
 * The execution nodes can be seen as a specialisation of the DCompNodes using the information about their position in the
 * Mscrs. For example a ParallelDo node will be transformed into a Mapper, a GbkMapper, a GbkReducer depending on its
 * position in the graph
 */
trait ExecutionPlan {

  type Term = CompNode

  def createExecutionPlan(computations: Seq[CompNode], mscrs: Set[Mscr]): Seq[Term] =
    rewrite(allStrategies(computations, mscrs))(computations)

  def allStrategies(outputs: Seq[CompNode], mscrs: Set[Mscr]): Rewriter.Strategy =
     attempt(convertFlatten) <*
     attempt(convertLoad)    <*
     attempt(convertReturn)

  def convertLoad = sometd(rule {
    case n @ comp.Load(_) => LoadExec(n)
  })

  def convertReturn = sometd(rule {
    case n @ comp.Return(_) => ReturnExec(n)
  })

  def convertFlatten = sometd(rule {
    case n @ comp.Flatten(ins) => FlattenExec(n, Vector(ins:_*))
  })
}

object ExecutionPlan extends ExecutionPlan

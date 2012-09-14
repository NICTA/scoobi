package com.nicta.scoobi
package impl
package exec

import plan._
import comp._
import graph.{MscrGraph, Mscr}
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Rewriter
import application.ScoobiConfiguration

/**
 * The execution transforms the DComp nodes as created by the user and the Mscrs computed by the MscrGraph
 * and creates a graph of ExecutionNodes which will map to the final MSCRS to be executed by Hadoop.
 *
 * The execution nodes can be seen as a specialisation of the DCompNodes using the information about their position in the
 * Mscrs. For example a ParallelDo node will be transformed into a Mapper, a GbkMapper, a GbkReducer depending on its
 * position in the graph
 */
trait ExecutionPlan extends MscrGraph {

  type Term = Any

  def createExecutionPlan(computations: Seq[CompNode], mscrs: Set[Mscr]): Seq[Term] =
    rewrite(allStrategies(computations, mscrs))(computations)

  def collectEnvironments(computations: Seq[CompNode], mscrs: Set[Mscr])(implicit sc: ScoobiConfiguration): Seq[Env[_]] =
    collectEnvs(createExecutionPlan(computations, mscrs))

  def allStrategies(outputs: Seq[CompNode], mscrs: Set[Mscr]): Rewriter.Strategy =
    all(convert)

  def attemptSomeTopdown(s : =>Strategy): Strategy =
    attempt(s <* some (attemptSomeTopdown (s)))

  def convert: Strategy = attemptSomeTopdown(rule {
    case n @ Materialize(in)                                    => MaterializeExec(Ref(n), in)
    case n @ Load(_)                                            => LoadExec(Ref(n))
    case n @ Return(_)                                          => ReturnExec(Ref(n))
    case n @ GroupByKey(in)                                     => GroupByKeyExec(Ref(n), in)
    case n @ Combine(in, f)                                     => CombineExec(Ref(n), in)
    case n @ Op(a, b, _)                                        => OpExec(Ref(n), a, b)
    case n @ Flatten(ins)                                       => FlattenExec(Ref(n), ins)
    case n @ ParallelDo(Load(_),_,_,_,_)                        => MapperExec(Ref(n), n.in)
    case n @ ParallelDo(ParallelDo(_,_,_,_,_),_,_,_,_)          => MapperExec(Ref(n), n.in)
    case n @ ParallelDo(Flatten(_),_,_,_,_)                     => MapperExec(Ref(n), n.in)
    case n @ ParallelDo(GroupByKey(_),_,_,_,_) if n -> isMapper => MapperExec(Ref(n), n.in)
    case n @ ParallelDo(Combine(_,_),_,_,_,_)  if n -> isMapper => MapperExec(Ref(n), n.in)
    case n @ ParallelDo(GroupByKey(_),_,_,_,_)                  => GbkReducerExec(Ref(n), n.in)
    case n @ ParallelDo(Combine(_,_),_,_,_,_)                   => ReducerExec(Ref(n), n.in)
    case n @ ParallelDo(in,_,_,_,_)                             => sys.error("a ParallelDo node can not have an input which is: "+in)
    case ns : Seq[_]                                            => ns // this allows to recurse into flatten inputs
  })

  def collectEnvs(nodes: Seq[Term])(implicit sc: ScoobiConfiguration): Seq[Env[_]] = {
    val envs = collect[Vector, Env[_]] {
      case n @ ReturnExec(_)        => n.env
      case n @ MaterializeExec(_,_) => n.env
      case n @ OpExec(_,_,_)        => n.env
    }
    nodes.foldLeft(Vector[Env[_]]())((res, cur) => res ++ envs(cur))
  }
}

object ExecutionPlan extends ExecutionPlan

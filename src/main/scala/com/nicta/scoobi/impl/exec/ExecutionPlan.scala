package com.nicta.scoobi
package impl
package exec

import plan.comp
import comp._
import comp.Combine
import comp.Flatten
import comp.GroupByKey
import comp.Load
import comp.Materialize
import comp.Op
import comp.Return
import plan.graph._
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Rewriter
import application.ScoobiConfiguration
import plan.graph.MapperInputChannel
import plan.graph.GbkOutputChannel

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

  /**
   * create an execution plan for a set of Mcrs
   */
  def createExecutionPlan(mscrs: Seq[Mscr]): Seq[Term] =
    rewrite(rewriteMscrs)(mscrs)

  /**
   * MSCR rewriting
   */
  /** rewrite all mscrs */
  def rewriteMscrs: Rewriter.Strategy =
    all(rewriteMscr)

  /** rewrite a Mscr and all its channels */
  def rewriteMscr: Strategy =
    rewriteSingleMscr <*
    rewriteChannels

  /** rewrite a single Mscr */
  def rewriteSingleMscr = rule {
    case m @ Mscr(in, out) => MscrExec(in, out)
  }


  /**
   * Channels rewriting
   */
  /** rewrite all input channels */
  def rewriteChannels: Rewriter.Strategy =
    everywhere(rewriteChannel)

  def rewriteChannel: Strategy =
    rewriteSingleChannel <*
    rewriteNodes         <* // rewrite the nodes in channels
    all(rewriteNodes)       // rewrite the remaining nodes which may be in Options (see GbkOutputChannel)

    /** rewrite one channel */
  lazy val rewriteSingleChannel: Strategy = {
    val tag = Tag()
    rule {
      // input channels
      case MapperInputChannel(pdos)     => MapperInputChannelExec(pdos.toSeq)
      case IdInputChannel(in)           => BypassInputChannelExec(in)
      case StraightInputChannel(in)     => StraightInputChannelExec(in)
      // output channels
      case GbkOutputChannel(g,f,c,r,s)  => GbkOutputChannelExec(g, f, c, r, s, tag.newTag)
      case FlattenOutputChannel(in,s)   => FlattenOutputChannelExec(in, s,     tag.newTag)
      case BypassOutputChannel(in,s)    => BypassOutputChannelExec(in, s,      tag.newTag)
    }
  }

  /**
   * Nodes rewriting
   */
  /** rewrite all nodes */
  def rewriteNodes: Rewriter.Strategy =
    all(rewriteNode)

  /** rewrite one node */
  def rewriteNode: Strategy = attemptSomeTopdown(rule {
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

  def attemptSomeTopdown(s : =>Strategy): Strategy =
    attempt(s <* some (attemptSomeTopdown (s)))

  // intermediary methods for testing
  def createExecutionPlanInputChannels(inputChannels: Seq[InputChannel]): Seq[Term] =
    rewrite(rewriteChannels)(inputChannels)

  def createExecutionGraph(computations: Seq[CompNode]): Seq[Term] =
    rewrite(rewriteNodes)(computations)

  def collectEnvironments(computations: Seq[CompNode])(implicit sc: ScoobiConfiguration): Seq[Env[_]] =
    collectEnvs(createExecutionGraph(computations))


  case class Tag() {
    var tag = 0
    def newTag = {
      val t = tag
      tag += 1
      t
    }
  }

}

object ExecutionPlan extends ExecutionPlan

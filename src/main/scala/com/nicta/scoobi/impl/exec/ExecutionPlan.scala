package com.nicta.scoobi
package impl
package exec

import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Rewriter
import org.kiama.attribution.Attribution._

import core._
import plan.comp._
import plan.mscr._

/**
 * The execution transforms the DComp nodes as created by the user and the Mscrs computed by the MscrGraph
 * and creates a graph of ExecutionNodes which will map to the final MSCRS to be executed by Hadoop.
 *
 * The execution nodes can be seen as a specialisation of the DCompNodes using the information about their position in the
 * Mscrs. For example a ParallelDo node will be transformed into a Mapper, a GbkMapper, a GbkReducer depending on its
 * position in the graph
 */
trait ExecutionPlan extends MscrMaker {

  type Term = Any

  /**
   * create an executable Mscr from an original one
   */
  def createExecutableMscr(mscr: Mscr): MscrExec =
    rewrite(rewriteMscr)(mscr).asInstanceOf[MscrExec]

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
    case m @ Mscr(in, out) => MscrExec(in.toSeq, out.toSeq)
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
    rule {
      // input channels
      case MapperInputChannel(pdos)        => MapperInputChannelExec(pdos.toSeq)
      case IdInputChannel(in, gbk)         => BypassInputChannelExec(in, gbk)
      case StraightInputChannel(in)        => StraightInputChannelExec(in)
      // output channels
      case ch @ GbkOutputChannel(g,f,c,r)  => GbkOutputChannelExec(g, f, c, r, ch.sinks)
      case ch @ FlattenOutputChannel(in)   => FlattenOutputChannelExec(in, ch.sinks)
      case ch @ BypassOutputChannel(in)    => BypassOutputChannelExec(in, ch.sinks)
    }

  }

  /** attribute returning a unique tag for an OutputChannel */
  lazy val tag: OutputChannelExec => Int = {
    val t = Tag()
    attr { case out  => out.setTag(t.newTag); out.tag }
  }
  
  /** attribute returning a unique tag for an OutputChannel */
  lazy val tags: MscrExec => InputChannelExec => Map[CompNode, Set[Int]] = {
    paramAttr { (mscr: MscrExec) => {
      case n @ MapperInputChannelExec(_) => Map(n.referencedNodes.map(pd => (pd, outputTags(pd, mscr))):_*) 
      case n                             => Map(n.referencedNode -> outputTags(n.referencedNode, mscr))
    }}
  }

  /** for a given node get the tags of the corresponding output channels */
  private[scoobi] def outputTags(node: CompNode, mscr: MscrExec): Set[Int] = {
    mscr.outputChannels.collect { case out: OutputChannelExec if (out.outputs diff (node -> outputs).toSeq).nonEmpty => tag(out) }.toSet
  }
  /**
   * Nodes rewriting
   */
  /** rewrite all nodes */
  def rewriteNodes: Rewriter.Strategy =
    all(rewriteNode)

  /** rewrite one node */
  def rewriteNode: Strategy = attemptSomeTopdown(rule {
    case n: Materialize[_]                 => MaterializeExec(Ref(n), n.in)
    case n: Load[_]                        => LoadExec(Ref(n))
    case n: Return[_]                      => ReturnExec(Ref(n))
    case n: GroupByKey[_,_]                => GroupByKeyExec(Ref(n), n.in)
    case n: Combine[_,_]                   => CombineExec(Ref(n),    n.in)
    case n: Op[_,_,_]                      => OpExec(Ref(n),         n.in1, n.in2)
    case n: Flatten[_]                     => FlattenExec(Ref(n),    n.ins)
    case n: ParallelDo[_,_,_]              => n.parent match {
      case g: GroupByKey[_,_]              => GbkMapperExec(Ref(n), Ref(g), n.in)
      case fl: Flatten[_] if isGroupByKey(fl.parent.asNode)  => GbkMapperExec(Ref(n), Ref(fl.parent.asInstanceOf[GroupByKey[_,_]]), n.in)
      case _                     => n.in match {
        case i: Load[_]            => MapperExec(Ref(n), i)
        case i: ParallelDo[_,_,_]  => MapperExec(Ref(n), i)
        case i: Flatten[_]         => MapperExec(Ref(n), i)
        case i: GroupByKey[_,_]    => if (n -> isMapper) MapperExec(Ref(n), i) else GbkReducerExec(Ref(n), i)
        case i: Combine[_,_]       => if (n -> isMapper) MapperExec(Ref(n), i) else GbkReducerExec(Ref(n), i)
        case i                     => sys.error("a ParallelDo node can not have an input which is: "+i)
      }
    }
    case ns : Seq[_]             => ns // this allows to recurse into flatten inputs
  })

  def attemptSomeTopdown(s : =>Strategy): Strategy =
    attempt(s <* some (attemptSomeTopdown (s)))

  // intermediary methods for testing
  def createExecutionPlanInputChannels(inputChannels: Seq[InputChannel]): Seq[Term] =
    rewrite(rewriteChannels)(inputChannels.map(initAttributable))

  def createExecutionGraph(computations: Seq[CompNode]): Seq[Term] =
    rewrite(rewriteNodes)(computations.map(initAttributable))

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

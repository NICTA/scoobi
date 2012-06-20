package com.nicta.scoobi
package impl
package plan
package graph

import comp._
import Optimiser._
import org.kiama.attribution.Attribution._
import org.kiama.attribution.{Attribution, Attributable}
import org.kiama.rewriting.Rewriter

trait MscrGraph {

  lazy val mscr : CompNode => Mscr =
    attr {
      /** create output channels for GroupByKey */
      case gbk @ GroupByKey(f @ Flatten(_))                   => createMscr(gbk, f).addGbkOutputChannel(gbk, f)
      case gbk @ GroupByKey(in)                               => createMscr(gbk, in).addGbkOutputChannel(gbk)

      /** add combiner to the output channel GroupByKey -> Combine */
      case cb @ Combine(gbk @ GroupByKey(_), _)               => (gbk -> mscr).addCombinerOnGbkOutputChannel(gbk, cb)
      case Combine(in, _)                                     => in -> mscr

      /**
       * add a reducer to the output channel if GroupByKey -> Combine -> ParallelDo, if one of these conditions on the ParallelDo is true:
       *
       * - it has a group barrier
       * - it has a fuse barrier
       * - it has no successor
       * - it is followed by a Materialize
       *
       * add a MapperInputChannel grouping all the ParallelDos sharing the same input
       */
      case pd @ ParallelDo(cb @ Combine(gbk @ GroupByKey(_),_),_,_,true,_)                                  => (cb -> mscr).addReducerOnGbkOutputChannel(gbk, pd)
      case pd @ ParallelDo(cb @ Combine(gbk @ GroupByKey(_),_),_,_,_,true)                                  => (cb -> mscr).addReducerOnGbkOutputChannel(gbk, pd)
      case pd @ ParallelDo(cb @ Combine(gbk @ GroupByKey(_),_),_,_,false,false) if !hasParent(pd)           => (cb -> mscr).addReducerOnGbkOutputChannel(gbk, pd)
      case pd @ ParallelDo(cb @ Combine(gbk @ GroupByKey(_),_),_,_,false,false) if isMaterialize(pd.parent) => (cb -> mscr).addReducerOnGbkOutputChannel(gbk, pd)
      case pd @ ParallelDo(in,_,_,_,_)                                                                      => (in -> mscr).addParallelDoOnMapperInputChannel(pd)

      case Flatten(ins)    => ins.map(_ -> mscr).headOption.getOrElse(Mscr())
      case Op(in1, in2, _) => in1 -> mscr
      case Materialize(in) => in -> mscr
      case Load(_)         => Mscr()
      case Return(_)       => Mscr()
    }

  def createMscr(gbk: GroupByKey[_,_], in: CompNode) =
    if (hasDescendentParallelDo(gbk) || hasDescendentGbk(gbk)) Mscr()
    else                                                        in -> mscr

  def hasDescendentParallelDo(node: CompNode) = descendents(node).collect(isAParallelDo).nonEmpty
  def hasDescendentGbk(node: CompNode) = descendents(node).collect(isAGroupByKey).nonEmpty

  def isAncestor(n: Attributable, other: Attributable): Boolean = other != null && n != null && !(other eq n) && ((other eq n.parent) || isAncestor(n.parent, other))
  def ancestors(n: Attributable): Seq[Attributable] = if (n.parent == null) Seq() else (n.parent +: ancestors(n.parent))
  def descendents(n: Attributable): Seq[Attributable] = n.children.toSeq ++ n.children.flatMap(descendents)
  def hasParent(node: CompNode) = Option(node.parent).isDefined


  private def isMaterialize(node: Attributable) = node match { case Materialize(_) => true; case _ => false }

  def mscrFor(node: CompNode) = {
    Attribution.initTree(node)
    node -> mscr
  }

  object Collector extends Rewriter {
    def mscrs(node: CompNode): Seq[Mscr] = (collectl {
      case n: Attributable => n.asInstanceOf[CompNode] -> mscr
    })(node)
  }
  def mscrsFor(nodes: CompNode*) = {
    nodes foreach mscrFor
    nodes.flatMap(n => Collector.mscrs(n)).toSet
  }
}

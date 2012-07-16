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

  lazy val mscr: CompNode => Mscr = (n: CompNode) => (n -> mscr1).copy(inputChannels = n -> mapperInputChannels, outputChannels = n -> gbkOutputChannels)
  lazy val mscr1: CompNode => Mscr =
    attr {
      case g @ GroupByKey(in)          => Mscr()
      case c @ Combine(in,_)           => in -> mscr1
      case Flatten(ins)                => ins.map(_ -> mscr1).headOption.getOrElse(Mscr())
      case Op(in1, in2, _)             => in1 -> mscr1; in2 -> mscr1
      case Materialize(in)             => in -> mscr1
      case pd @ ParallelDo(in,_,_,_,_) => in -> mscr1
      case other                       => Mscr()
    }

  lazy val mapperInputChannels: CompNode => Seq[MapperInputChannel] = (n: CompNode) => distinct(n -> mapperInputChannels1)
  lazy val mapperInputChannels1: CompNode => Seq[MapperInputChannel] =
    attr {
      case pd @ ParallelDo(in,_,_,_,_) if (parallelDoIsMapper(pd)) => Seq(MapperInputChannel(distinctNodes(pd +: (pd -> siblings).collect(isAParallelDo))))
      case pd @ ParallelDo(in,_,_,_,_)                             => in -> mapperInputChannels1
      case GroupByKey(in)                                          => in -> mapperInputChannels1
      case Combine(in,_)                                           => in -> mapperInputChannels1
      case Flatten(ins)                                            => ins.flatMap(_ -> mapperInputChannels1)
      case Op(in1, in2, _)                                         => (in1 -> mapperInputChannels1) ++ (in2 -> mapperInputChannels1)
      case Materialize(in)                                         => in -> mapperInputChannels1
      case other                                                   => Seq()
    }


  lazy val gbkOutputChannels: CompNode => Seq[GbkOutputChannel] = (n: CompNode) => (n -> gbkOutputChannels1).map(out => (out.groupByKey.id, out)).toMap.values.toSeq
  lazy val gbkOutputChannels1: CompNode => Seq[GbkOutputChannel] =
    attr {
      case g @ GroupByKey(in @ Flatten(_))                        => Seq(GbkOutputChannel(g, flatten = Some(in)))
      case g @ GroupByKey(in)                                     => Seq(GbkOutputChannel(g))
      case cb @ Combine(in,_) if (cb -> isGbkOutput)              => (in -> gbkOutputChannels1).map(_.set(cb))
      case pd @ ParallelDo(in,_,_,_,_) if parallelDoIsReducer(pd) => (in -> gbkOutputChannels1).map(_.set(pd))
      case Flatten(ins)                                           => ins.flatMap(in => in -> gbkOutputChannels1)
      case Op(in1, in2, _)                                        => (in1 -> gbkOutputChannels1) ++ (in2 -> gbkOutputChannels1)
      case Materialize(in)                                        => in -> gbkOutputChannels1
      case other                                                  => Seq()
    }

  lazy val isGbkOutput: CompNode => Boolean =
    childAttr { node: CompNode => parent: Attributable => {
      if (parent == null) true
      else
        node match {
          case pd @ ParallelDo(cb @ Combine(_,_),_,_,true,_) => true
          case pd @ ParallelDo(cb @ Combine(_,_),_,_,_,true) => true
          case pd @ ParallelDo(in,_,_,_,_)                   => parent.asNode -> isGbkOutput
          case Combine(in,_)                                 => parent.asNode -> isGbkOutput
          case Flatten(ins)                                  => parent.asNode -> isGbkOutput
          case Op(in1, in2, _)                               => parent.asNode -> isGbkOutput
          case Materialize(in)                               => parent.asNode -> isGbkOutput
          case GroupByKey(in)                                => false
          case other                                         => false
        }
    }}

  def parallelDoIsMapper(pd: ParallelDo[_,_,_])  = !parallelDoIsReducer(pd)
  def parallelDoIsReducer(pd: ParallelDo[_,_,_]) = pd -> isGbkOutput

  lazy val inputs : CompNode => Seq[CompNode] =
    attr {
      case GroupByKey(in)         => Seq(in)
      case Combine(in, _)         => Seq(in)
      case ParallelDo(in,_,_,_,_) => Seq(in)
      case Flatten(ins)           => ins
      case Op(in1, in2, _)        => Seq(in1, in2)
      case Materialize(in)        => Seq(in)
      case Load(_)                => Seq()
      case Return(_)              => Seq()
    }

  lazy val outputs : CompNode => Seq[CompNode] =
    attr {
      case node: CompNode => distinctNodes((node -> parents).collect { case a if (a -> inputs).exists(_ eq node) => a })
    }

  lazy val sharedInputs : CompNode => Seq[CompNode] =
    attr {
      case node: CompNode => distinctNodes((node -> inputs).collect { case in if (in -> outputs).filterNot(_ eq node).nonEmpty => in })
    }

  lazy val siblings : CompNode => Seq[CompNode] =
    attr {
      case node: CompNode => distinctNodes((node -> inputs).flatMap(in => (in -> outputs).filterNot(_ eq node)))
    }

  lazy val descendents : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => {
        distinctNodes(node.children.toSeq ++ node.children.flatMap(_.children))
      }
    }

  def canReach(n: CompNode): CompNode => Boolean =
    paramAttr { target: CompNode =>
      node: CompNode => descendents(node).map(_.id).contains(target.id)
    }(n)

  def distinct(ins: Seq[MapperInputChannel]): Seq[MapperInputChannel] =
    ins.map(in => (in.parDos.map(_.id).toSet, in)).toMap.values.toSeq

  def distinctNodes[T <: CompNode](nodes: Seq[Attributable]): Seq[T] =
    nodes.map(n => (n.asInstanceOf[T].id, n.asInstanceOf[T])).toMap.values.toSeq

  lazy val ancestors : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => {
        val p = Option(node.parent).map(n => n.asNode)
        p.toSeq ++ p.toSeq.flatMap { parent => ancestors(parent) }
      }
    }

  lazy val parents : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => {
        val p = Option(node.parent).map(n => n.asNode)
        p.toSeq ++ distinctNodes(p.toSeq.flatMap { parent =>
          ancestors(parent).flatMap { a => descendents(a).filter(canReach(node)) }
        })
      }
    }

  def createMscr(gbk: GroupByKey[_,_], in: CompNode) =
    if (hasDescendentParallelDo(gbk) || hasDescendentGbk(gbk)) { in -> mscr; Mscr() }
    else                                                         in -> mscr

  def hasDescendentParallelDo(node: CompNode) = descendents(node).collect(isAParallelDo).nonEmpty
  def hasDescendentGbk(node: CompNode)        = descendents(node).collect(isAGroupByKey).nonEmpty

  def isAncestor(n: Attributable, other: Attributable): Boolean = other != null && n != null && !(other eq n) && ((other eq n.parent) || isAncestor(n.parent, other))

  /**
   * In the context of a CompNode graph there can be several outputs reachable from a given node
   */
  def reachableOutputs(n: CompNode): Seq[Attributable] = ancestors(n).filter(a => reachableInputs(a).contains(n))

  /** alias for descendents in the context of a CompNode graph */
  def reachableInputs(n: CompNode) = n -> descendents
  def hasParent(node: CompNode) = Option(node.parent).isDefined

  implicit def asCompNode(a: Attributable): AsCompNode = AsCompNode(a)
  case class AsCompNode(a: Attributable) {
    def asNode = a.asInstanceOf[CompNode]
  }
  private def isMaterialize(node: Attributable) = node match { case Materialize(_) => true; case _ => false }

  def mscrFor(node: CompNode) = {
    Attribution.initTree(node)
    Optimiser.optimise(node) -> mscr
  }

  def mscrsFor(nodes: CompNode*) = nodes map mscrFor
}

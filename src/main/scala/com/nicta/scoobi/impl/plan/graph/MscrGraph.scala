package com.nicta.scoobi
package impl
package plan
package graph

import comp._
import Optimiser._
import org.kiama.attribution.Attribution._
import org.kiama.attribution.{Attribution, Attributable}
import org.kiama.rewriting.Rewriter

/**
 * This trait computes the Mscr for a given nodes graph.
 *
 * It does so by collecting:
 *
 *  - the input channels going into a "related" set of GroupByKey nodes
 *  - the output channels coming out of a "related" set of GroupByKey nodes
 *
 *  2 GroupByKey nodes are being "related" if they have input ParallelDos sharing themselves the same inputs.
 *  Those ParallelDo nodes are said to be "siblings" in the code below
 */
trait MscrGraph {
  /**
   * compute the mscr of a node
   */
  lazy val mscr: CompNode => Mscr = (n: CompNode) => {
    val m = n -> mscr1
    m.inputChannels  = n -> mapperInputChannels
    m.outputChannels = n -> gbkOutputChannels
    m
  }

  /**
   * there must be a new Mscr for each GroupByKey
   */
  lazy val mscr1: CompNode => Mscr =
    attr {
      case g @ GroupByKey(in)          => Mscr()
      case c @ Combine(in,_)           => in -> mscr1
      case Flatten(ins)                => ins.map(_ -> mscr1).headOption.getOrElse(Mscr())
      case Op(in1, in2, _)             => in1 -> mscr1; in2 -> mscr1 // *both* mscrs are evaluated, so that all the graph is evaluated
      case Materialize(in)             => in -> mscr1
      case pd @ ParallelDo(in,_,_,_,_) => in -> mscr1
      case other                       => Mscr()
    }

  /**
   * compute the mapper input channels of a node. They must be distinct
   */
  lazy val mapperInputChannels: CompNode => Seq[MapperInputChannel] = (n: CompNode) => distinct(n -> mapperInputChannels1)

  /**
   * the mapper input channel of a node is grouping ParallelDo nodes which are siblings
   */
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

  /**
   * compute the gbk output channels of a node. They must be distinct
   */
  lazy val gbkOutputChannels: CompNode => Seq[GbkOutputChannel] = (n: CompNode) => distinct(n -> gbkOutputChannels1)
  /**
   * the gbk output channel of a node is associating a given GroupByKey with possibly:
   *  - a Flatten node if it is an input of the GroupByKey
   *  - a Combine node if it is the output of the GroupByKey
   *  - a ParallelDo node (called a "reducer") if it is the output of the GroupByKey
   */
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

  /**
   * compute if a node is the output of a GroupByKey.
   *  - if the node is a ParallelDo with a group barrier
   *  - if the node is a ParallelDo with a fuse barrier
   *  - if the node parent is a GroupByKey output
   *  - if there is no node parent
   *
   *  For example, if the graph is: gbk(pd1) then pd1 -> isGbkOutput == false because pd1.parent is a GroupByKey
   */
  lazy val isGbkOutput: CompNode => Boolean =
    childAttr { node: CompNode => parent: Attributable => {
      if (!hasParent(node)) true
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

  /** a ParallelDo is a reducer if it is not the output of a GroupByKey */
  def parallelDoIsReducer(pd: ParallelDo[_,_,_]) = pd -> isGbkOutput
  /** a ParallelDo is a mapper, i.e a GroupByKey input, when it's not a reducer */
  def parallelDoIsMapper(pd: ParallelDo[_,_,_])  = !parallelDoIsReducer(pd)
  /** @return a sequence of distinct mapper input channels */
  def distinct(ins: Seq[MapperInputChannel]): Seq[MapperInputChannel] =
    ins.map(in => (in.parDos.map(_.id).toSet, in)).toMap.values.toSeq
  /** @return a sequence of distinct group by key output channels */
  def distinct(out: Seq[GbkOutputChannel], dummy: Int = 0): Seq[GbkOutputChannel] =
    out.map(o => (o.groupByKey.id, o)).toMap.values.toSeq

  /** compute the inputs of a given node */
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

  /**
   *  compute the outputs of a given node.
   *  They are all the parents of the node where the parent inputs contain this node.
   */
  lazy val outputs : CompNode => Seq[CompNode] =
    attr {
      case node: CompNode => distinctNodes((node -> parents).collect { case a if (a -> inputs).exists(_ eq node) => a })
    }

  /**
   *  compute the shared input of a given node.
   *  They are all the distinct inputs of a node which are also inputs of another node
   */
  lazy val sharedInputs : CompNode => Seq[CompNode] =
    attr {
      case node: CompNode => distinctNodes((node -> inputs).collect { case in if (in -> outputs).filterNot(_ eq node).nonEmpty => in })
    }

  /**
   *  compute the siblings of a given node.
   *  They are all the nodes which shared at least one input with this node
   */
  lazy val siblings : CompNode => Seq[CompNode] =
    attr {
      case node: CompNode => distinctNodes((node -> inputs).flatMap(in => (in -> outputs).filterNot(_ eq node)))
    }

  /**
   * compute all the descendents of a node
   * They are all the recursive children reachable from this node
   */
  lazy val descendents : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => {
        distinctNodes(node.children.toSeq ++ node.children.flatMap(_.children))
      }
    }

  /** @return a function returning true if one node can be reached from another, i.e. it is in the list of its descendents */
  def canReach(n: CompNode): CompNode => Boolean =
    paramAttr { target: CompNode =>
      node: CompNode => descendents(node).map(_.id).contains(target.id)
    }(n)

  /** @return a sequence of distinct nodes */
  def distinctNodes[T <: CompNode](nodes: Seq[Attributable]): Seq[T] =
    nodes.map(n => (n.asInstanceOf[T].id, n.asInstanceOf[T])).toMap.values.toSeq

  /**
   * compute the ancestors of a node, that is all the direct parents of this node up to a root of the graph
   */
  lazy val ancestors : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => {
        val p = Option(node.parent).map(n => n.asNode)
        p.toSeq ++ p.toSeq.flatMap { parent => ancestors(parent) }
      }
    }

  /**
   * compute all the parents of a given node. A node A is parent of a node B if B can be reached from A
   *
   */
  lazy val parents : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => {
        val p = Option(node.parent).map(n => n.asNode)
        p.toSeq ++ distinctNodes(p.toSeq.flatMap { parent =>
          ancestors(parent).flatMap { a => descendents(a).filter(canReach(node)) }
        })
      }
    }

  /**
   * compute all the nodes which compose this graph
   */
  lazy val vertices : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => (node +: node.children.flatMap(n => n.asNode -> vertices).toSeq) ++ node.children.map(_.asNode).toSeq
    }

  /**
   * compute all the edges which compose this graph
   */
  lazy val edges : CompNode => Seq[(CompNode, CompNode)] =
    circular(Seq[(CompNode, CompNode)]()) {
      case node: CompNode => node.children.map(n => node -> n.asNode).toSeq ++ node.children.flatMap(n => n.asNode -> edges).toSeq
    }

  /** @return true if a node is the ancestor of another */
  def isAncestor(n: Attributable, other: Attributable): Boolean = other != null && n != null && !(other eq n) && ((other eq n.parent) || isAncestor(n.parent, other))

  /** alias for descendents in the context of a CompNode graph */
  def reachableInputs(n: CompNode) = n -> descendents
  /** @return true if a node has a parent */
  def hasParent(node: CompNode) = Option(node.parent).isDefined

  /**
   * syntax enhancement to force the conversion of an Attributable node to a CompNode
   */
  implicit def asCompNode(a: Attributable): AsCompNode = AsCompNode(a)
  case class AsCompNode(a: Attributable) {
    def asNode = a.asInstanceOf[CompNode]
  }

  /** @return the MSCR for a given node */
  def computeMscrFor(node: CompNode) = {
    // make sure that the parent <-> children relationships are initialized
    Attribution.initTree(node)
    node -> mscr
  }

  /** @return the MSCR for a given node. Used for testing only because it does the optimisation first */
  private[plan] def mscrFor(node: CompNode) = computeMscrFor(Optimiser.optimise(node))
  /** @return the MSCRs for a list of nodes. Used for testing only */
  private[plan] def mscrsFor(nodes: CompNode*) = nodes map mscrFor
}

object MscrGraph extends MscrGraph
package com.nicta.scoobi
package impl
package plan
package comp

import scala.collection.immutable.SortedSet
import org.kiama.attribution.Attribution._
import org.kiama.attribution.Attributable

import core._
import control.Exceptions._
import collection._
import IdSet._

/**
 * General methods for navigating a graph of CompNodes
 */
trait CompNodes {
  /** @return a sequence of distinct nodes */
  def distinctNodes[T <: CompNode](nodes: Seq[Attributable]): Set[T] =
    nodes.asNodes.map(n => (n.asInstanceOf[T].id, n.asInstanceOf[T])).toMap.values.toSet

  /** @return true if a node is the ancestor of another */
  def isAncestor(n: Attributable, other: Attributable): Boolean = other != null && n != null && !(other eq n) && ((other eq n.parent) || isAncestor(n.parent, other))

  /** @return true if a node has a parent */
  def hasParent(node: CompNode) = Option(node.parent).isDefined

  /**
   * syntax enhancement to force the conversion of an Attributable node to a CompNode
   */
  implicit def asCompNode(a: Attributable): AsCompNode = AsCompNode(a)
  case class AsCompNode(a: Attributable) {
    def asNode = a.asInstanceOf[CompNode]
  }

  /**
   * syntax enhancement to force the conversion of an iterator of Attributable nodes
   * (as returned by 'childeren' for example) to a list of CompNodes
   */
  implicit def asCompNodes(as: Iterator[Attributable]): AsCompNodes = AsCompNodes(as.toSeq)
  implicit def asCompNodes(as: Seq[Attributable]): AsCompNodes = AsCompNodes(as)
  case class AsCompNodes(as: Seq[Attributable]) {
    def asNodes = as.collect { case c: CompNode => c }
  }

  /** return true if a CompNodeis a Flatten */
  lazy val isFlatten: CompNode => Boolean = { case f: Flatten[_] => true; case other => false }
  /** return true if a CompNodeis a ParallelDo */
  lazy val isParallelDo: CompNode => Boolean = { case p: ParallelDo[_,_,_] => true; case other => false }
  /** return true if a CompNodeis a Flatten */
  lazy val isAFlatten: PartialFunction[Any, Flatten[_]] = { case f: Flatten[_] => f }
  /** return true if a CompNodeis a ParallelDo */
  lazy val isAParallelDo: PartialFunction[Any, ParallelDo[_,_,_]] = { case p: ParallelDo[_,_,_] => p }
  /** return true if a CompNodeis a GroupByKey */
  lazy val isGroupByKey: CompNode => Boolean = { case g: GroupByKey[_,_] => true; case other => false }
  /** return true if a CompNodeis a GroupByKey */
  lazy val isAGroupByKey: PartialFunction[Any, GroupByKey[_,_]] = { case gbk: GroupByKey[_,_] => gbk }
  /** return true if a CompNodeis a Materialize */
  lazy val isMaterialize: CompNode => Boolean = { case m: Materialize[_] => true; case other => false }
  /** return true if a CompNodeis a Return */
  lazy val isReturn: CompNode => Boolean = { case r: Return[_]=> true; case other => false }
  /** return true if a CompNode is an Op */
  lazy val isOp: CompNode => Boolean = { case o: Op[_,_,_] => true; case other => false }
  /** return true if a CompNode has a cycle in its graph */
  lazy val isCyclic: CompNode => Boolean = (n: CompNode) => tryKo(n -> descendents)

  /** compute the inputs of a given node */
  lazy val inputs : CompNode => SortedSet[CompNode] = attr {
    case n  => n.children.asNodes.toIdSet
  }

  /**
   *  compute the outputs of a given node.
   *  They are all the parents of the node where the parent inputs contain this node.
   */
  lazy val outputs : CompNode => SortedSet[CompNode] = attr {
    case node: CompNode => (node -> parents) collect { case a if (a -> inputs).exists(_ eq node) => a }
  }

  /**
   *  compute the shared input of a given node.
   *  They are all the distinct inputs of a node which are also inputs of another node
   */
  lazy val sharedInputs : CompNode => SortedSet[CompNode] = attr {
    case node: CompNode => ((node -> inputs).collect { case in if (in -> outputs).filterNot(_ eq node).nonEmpty => in })
  }

  /**
   *  compute the siblings of a given node.
   *  They are all the nodes which share at least one input with this node
   */
  lazy val siblings : CompNode => SortedSet[CompNode] = attr {
    case node: CompNode => (node -> inputs).flatMap { in => (in -> outputs) }.filterNot(_ eq node)
  }

  /** @return true if a node has siblings */
  lazy val hasSiblings : CompNode => Boolean = attr { case node: CompNode => (node -> siblings).nonEmpty }
  /**
   * compute all the descendents of a node
   * They are all the recursive children reachable from this node */
  lazy val descendents : CompNode => SortedSet[CompNode] =
    attr { case node: CompNode => (node -> nonUniqueDescendents).toIdSet }

  private lazy val nonUniqueDescendents : CompNode => Seq[CompNode] =
    attr { case node: CompNode => (node.children.asNodes ++ node.children.asNodes.flatMap(nonUniqueDescendents)) }

  /** @return a function returning true if one node can be reached from another, i.e. it is in the list of its descendents */
  def canReach(n: CompNode): CompNode => Boolean =
    paramAttr { target: CompNode =>
      node: CompNode => descendents(node).contains(target)
    }(n)

  /** compute the ancestors of a node, that is all the direct parents of this node up to a root of the graph */
  lazy val ancestors : CompNode => SortedSet[CompNode] =
    circular(IdSet.empty[CompNode]: SortedSet[CompNode]) {
      case node: CompNode => {
        val p = Option(node.parent).toSeq.asNodes
        (p ++ p.flatMap { parent => ancestors(parent) }).toIdSet
      }
    }

  /** compute all the parents of a given node. A node A is parent of a node B if B can be reached from A */
  lazy val parents : CompNode => SortedSet[CompNode] =
    circular(IdSet.empty[CompNode]: SortedSet[CompNode]) {
      case node: CompNode => {
        (node -> ancestors).flatMap { ancestor =>
          ((ancestor -> descendents) + ancestor).filter(canReach(node))
        }
      }
    }

  /** @return an option for the potentially missing parent of a node */
  lazy val parentOpt: CompNode => Option[CompNode] = attr { case n => Option(n.parent).map(_.asNode) }

  /** compute the vertices starting from a node */
  lazy val vertices : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => ((node +: node.children.asNodes.flatMap(n => n -> vertices).toSeq) ++ node.children.asNodes).
        toIdSet.toSeq // make the vertices unique
    }

  /** compute all the edges which compose this graph */
  lazy val edges : CompNode => Seq[(CompNode, CompNode)] =
    circular(Seq[(CompNode, CompNode)]()) {
      case node: CompNode => (node.children.asNodes.map(n => node -> n) ++ node.children.asNodes.flatMap(n => n -> edges).toSeq).
        map { case (a, b) => (a.id, b.id) -> (a, b) }.toMap.values.toSeq // make the edges unique
    }
}
object CompNodes extends CompNodes

package com.nicta.scoobi
package impl
package plan
package comp

import org.kiama.attribution.{Attribution, Attributable}

import core._
import control.Exceptions._
import control.Functions._
import collection._

/**
 * General methods for navigating a graph of CompNodes
 */
trait CompNodes extends Attribution {
  /** @return a sequence of distinct nodes */
  def distinctNodes[T <: CompNode](nodes: Seq[Attributable]): Seq[T] =
    nodes.map(n => (n.asInstanceOf[T].id, n.asInstanceOf[T])).toMap.values.toSeq

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
   * (as returned by 'children' for example) to a list of CompNodes
   */
  implicit def asCompNodes(as: Iterator[Attributable]): AsCompNodes = AsCompNodes(as.toSeq)
  implicit def asCompNodes(as: Seq[Attributable]): AsCompNodes = AsCompNodes(as)
  case class AsCompNodes(as: Seq[Attributable]) {
    def asNodes: Seq[CompNode] = as.collect { case c: CompNode => c }
  }

  /** return true if a CompNode is a Flatten */
  lazy val isFlatten: CompNode => Boolean = { case f: Flatten[_] => true; case other => false }
  /** return true if a CompNode is a ParallelDo */
  lazy val isParallelDo: CompNode => Boolean = { case p: ParallelDo[_,_,_] => true; case other => false }
  /** return true if a CompNode is a Load */
  lazy val isLoad: CompNode => Boolean = { case l: Load[_] => true; case other => false }
  /** return true if a CompNode is a Flatten */
  lazy val isAFlatten: PartialFunction[Any, Flatten[_]] = { case f: Flatten[_] => f }
  /** return true if a CompNode is a Flatten */
  lazy val isACombine: PartialFunction[Any, Combine[_,_]] = { case c: Combine[_,_] => c }
  /** return true if a CompNode is a ParallelDo */
  lazy val isAParallelDo: PartialFunction[Any, ParallelDo[_,_,_]] = { case p: ParallelDo[_,_,_] => p }
  /** return true if a CompNode is a GroupByKey */
  lazy val isGroupByKey: CompNode => Boolean = { case g: GroupByKey[_,_] => true; case other => false }
  /** return true if a CompNode is a GroupByKey */
  lazy val isAGroupByKey: PartialFunction[Any, GroupByKey[_,_]] = { case gbk: GroupByKey[_,_] => gbk }
  /** return true if a CompNode is a Materialize */
  lazy val isMaterialize: CompNode => Boolean = { case m: Materialize[_] => true; case other => false }
  /** return true if a CompNode is a Return */
  lazy val isReturn: CompNode => Boolean = { case r: Return[_]=> true; case other => false }
  /** return true if a CompNode needs to be persisted */
  lazy val isSinkNode: CompNode => Boolean = isMaterialize
  /** return true if a CompNode needs to be loaded */
  lazy val isSourceNode: CompNode => Boolean = isReturn || isMaterialize || isOp || isLoad
  /** return true if a CompNode is an Op */
  lazy val isOp: CompNode => Boolean = { case o: Op[_,_,_] => true; case other => false }
  /** return true if a CompNode has a cycle in its graph */
  lazy val isCyclic: CompNode => Boolean = (n: CompNode) => tryKo(n -> descendents)
  /** return true if a CompNode doesn't have a cycle in its graph */
  lazy val isNotCyclic: CompNode => Boolean = (n: CompNode) => !isCyclic(n)

  /** compute the inputs of a given node */
  lazy val inputs : CompNode => Seq[CompNode] = attr {
    // for a parallel do node just consider the input node, not the environment
    case pd: ParallelDo[_,_,_]  => Seq(pd.in)
    case n                      => n.children.asNodes
  }

  /** compute the incoming data of a given node: all the inputs + possible environment for a parallelDo */
  lazy val incomings : CompNode => Seq[CompNode] = attr {
    case n                      => n.children.asNodes
  }

  /**
   *  compute the outputs of a given node.
   *  They are all the parents of the node where the parent inputs contain this node.
   */
  lazy val outputs : CompNode => Seq[CompNode] = attr {
    case node: CompNode => (node -> parents) collect { case a if (a -> inputs).exists(_ eq node) => a }
  }

  /** compute the outcoming data of a given node: all the outputs + possible environment for a parallelDo */
  lazy val outgoings : CompNode => Seq[CompNode] = attr {
    case node: CompNode => (node -> parents) collect { case a if (a -> incomings).exists(_ eq node) => a }
  }

  /** all inputs and outputs */
  lazy val inputsOutputs: CompNode => Seq[CompNode] = attr { case n => (n -> inputs) ++ (n -> outputs) }

  /**
   *  compute the uses of a given node.
   *  i.e. the outputs of a node + its uses as an environment is parallelDos
   */
  lazy val uses : CompNode => Seq[CompNode] = attr {
    case node: CompNode => (node -> outgoings) collect { case a if (a -> incomings).exists(_ eq node)=> a }
  }

  /**
   *  compute the shared input of a given node.
   *  They are all the distinct inputs of a node which are also inputs of another node
   */
  lazy val sharedInputs : CompNode => Seq[CompNode] = attr {
    case node: CompNode => ((node -> inputs).collect { case in if (in -> outputs).filterNot(_ eq node).nonEmpty => in })
  }
  /**
   * compute all the descendents of a node
   * They are all the recursive children reachable from this node */
  lazy val descendents : CompNode => Seq[CompNode] =
    attr { case node: CompNode =>
      val children = node.children.asNodes
      val childrenDescendents = distinctNodes(children.flatMap(descendents))
      children ++ childrenDescendents
    }

  type Predicate = CompNode => Boolean
  /**
   * compute all the descendents of a node while some criterion is true
   */
  lazy val descendentsWhile: Predicate => CompNode => Seq[CompNode] =
    paramAttr { predicate: Predicate => node: CompNode =>
      node -> nonUniqueDescendentsWhile(predicate)
    }

  private lazy val nonUniqueDescendentsWhile: Predicate => CompNode => Seq[CompNode] =
    paramAttr { predicate: Predicate =>
      (node: CompNode) => {
        val childrenWhile = node.children.asNodes.filter(predicate)
        childrenWhile ++ distinctNodes(childrenWhile.flatMap(nonUniqueDescendentsWhile(predicate)))
      }
    }

  /**
   * compute all the descendents of a node until some criterion is true
   */
  lazy val descendentsUntil: Predicate => CompNode => Seq[CompNode] =
    paramAttr { predicate: Predicate => node: CompNode =>
      node -> nonUniqueDescendentsUntil(predicate)
    }

  private lazy val nonUniqueDescendentsUntil: Predicate => CompNode => Seq[CompNode] =
    paramAttr { predicate: Predicate =>
      (node: CompNode) => {
        val (childrenWhile, childrenUntil) = node.children.asNodes.span(predicate)
        val children = childrenWhile ++ childrenUntil.headOption.toSeq
        children ++ distinctNodes(children.flatMap(nonUniqueDescendentsUntil(predicate)))
      }
    }

  lazy val descendentsWhileUntil: ((Predicate, Predicate)) => CompNode => Seq[CompNode] =
    paramAttr { predicates: ((Predicate, Predicate)) => node: CompNode =>
      node -> nonUniqueDescendentsWhileUntil(predicates)
    }

  private lazy val nonUniqueDescendentsWhileUntil: ((Predicate, Predicate)) => CompNode => Seq[CompNode] =
    paramAttr { predicates: ((Predicate, Predicate)) =>
      val (whilePredicate, untilPredicate) = predicates
      (node: CompNode) => {
        val (childrenWhile, childrenUntil) = node.children.asNodes.filter(whilePredicate).span(untilPredicate)
        val children = distinctNodes(childrenWhile ++ childrenUntil.headOption.toSeq)
        children ++ children.flatMap(nonUniqueDescendentsWhileUntil(predicates))
      }
    }
  /** @return a function returning true if one node can be reached from another, i.e. it is in the list of its descendents */
  def canReach(n: CompNode): CompNode => Boolean =
    paramAttr { target: CompNode =>
      node: CompNode => descendents(node).contains(target)
    }(n)

  /** compute the ancestors of a node, that is all the direct parents of this node up to a root of the graph */
  lazy val ancestors : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => {
        val p = Option(node.parent).toSeq.asNodes
        p ++ p.flatMap { parent => ancestors(parent) }
      }
    }

  /** compute all the parents of a given node. A node A is parent of a node B if B can be reached from A */
  lazy val parents : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => {
        distinctNodes((node -> ancestors).flatMap { ancestor =>
          ((ancestor -> descendents) :+ ancestor).filter(canReach(node))
        })
      }
    }

  /** @return true if 1 node is parent of the other, or if they are the same node */
  lazy val isParentOf = paramAttr {(other: CompNode) => node: CompNode =>
    (node -> isStrictParentOf(other)) || (node.id == other.id)
  }

  /** @return true if 1 node is parent of the other, or but not  the same node */
  lazy val isStrictParentOf = paramAttr { (other: CompNode) => node: CompNode =>
    (node -> descendents).contains(other) || (other -> descendents).contains(node)
  }

  /** @return an option for the potentially missing parent of a node */
  lazy val parentOpt: CompNode => Option[CompNode] = attr { case n => Option(n.parent).map(_.asNode) }

  /** compute the vertices starting from a node */
  lazy val vertices : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode =>
        distinctNodes((node +: node.children.asNodes.flatMap(n => n -> vertices).toSeq) ++ node.children.asNodes) // make the vertices unique
    }

  /** compute all the edges which compose this graph */
  lazy val edges : CompNode => Seq[(CompNode, CompNode)] =
    circular(Seq[(CompNode, CompNode)]()) {
      case node: CompNode =>
        (node.children.asNodes.map(n => node -> n) ++ node.children.asNodes.flatMap(n => n -> edges)).
           map { case (a, b) => (a.id, b.id) -> (a, b) }.toMap.values.toSeq // make the edges unique
    }

  /** compute all the nodes which use a given node as an environment */
  lazy val usesAsEnvironment : CompNode => Seq[ParallelDo[_,_,_]] =
    attr {
      case node: CompNode => (node -> outgoings).collect(isAParallelDo).toSeq.filter(_.env == node)
    }

  /** initialize the Kiama attributes */
  def initAttributable[T <: Attributable](t: T): T  = {
    if (t.children == null || !t.children.hasNext) {
      initTree(t)
    }
    t
  }

  type GBK = GroupByKey[_,_]

}
object CompNodes extends CompNodes

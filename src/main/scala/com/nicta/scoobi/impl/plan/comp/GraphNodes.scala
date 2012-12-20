package com.nicta.scoobi
package impl
package plan
package comp

import scalaz.Scalaz._
import scalaz.std.vector._
import org.kiama.attribution.{Attributable, Attribution}
import control.Exceptions._

/**
 * generic functions for a nodes graph
 */
trait GraphNodes extends Attribution {
  type T <: Attributable

  /** compute the parent of a node */
  lazy val parent : T => Option[T] =
    attr("parent") { case node => Option(node.parent.asInstanceOf[T]) }

  /** compute the children of a node */
  lazy val children : T => Seq[T] =
    attr("children") { case node => Vector(node.children.toSeq.map(_.asInstanceOf[T]):_*) }

  /** the root of the graph, computed from a given node */
  lazy val root : T => T = 
    attr("root") { case node => parent(node).map(root).getOrElse(node) }
  
  /**
   * compute all the descendents of a node
   * They are all the recursive children reachable from this node
   */
  lazy val descendents : T => Seq[T] = attr("descendents") { case node =>
    (children(node) ++ children(node).flatMap(descendents)).distinct
  }

  /** compute the parents of a node, that is all the chain of parents from this node up to the root of the graph */
  lazy val parents : T => Seq[T] = attr("parents") { case node =>
    val p = parent(node).toSeq
    p ++ p.flatMap(parents)
  }

  /**
   * the nodes which have this node as a direct child
   *
   * for efficiency, this uses a table of all the nodes using a given one, computed from the root
   */
  lazy val uses : T => Set[T] = attr("uses") { case node =>
    usesTable(node -> root).getOrElse(node, Set())
  }

  /** a Map of all the nodes which are using a given node */
  private lazy val usesTable : T => Map[T, Set[T]] = attr("usesTable") { case node =>
    Vector(children(node):_*).foldMap((child: T) => usesTable(child) |+| Map(child -> Set(node)))
  }

  /** compute all the indirect uses of a given node, that is all the nodes which have this node as a descendent */
  lazy val transitiveUses : T => Set[T] = attr("transitiveUses") { case node =>
    (uses(node) ++ uses(node).flatMap(_ -> transitiveUses)).toSet
  }

  /**
   * return true if a CompNode has a cycle in its graph,
   * this will be detected by Kiama throwing an exception when fetching the descendents of a node
   */
  lazy val isCyclic: T => Boolean = (n: T) => tryKo(n -> descendents)

  /** @return true if 1 node is parent of the other, or if they are the same node */
  private[impl]
  lazy val isParentOf = paramAttr("isParentOf") { (other: T) => node: T =>
    (node -> isStrictParentOf(other)) || (node == other)
  }

  /** @return true if 1 node is parent of the other, or but not  the same node */
  private[impl]
  lazy val isStrictParentOf = paramAttr("isStrictParentOf") { (other: T) => node: T =>
    (node -> descendents).contains(other) || (other -> descendents).contains(node)
  }

  /** compute the vertices starting from a node */
  private[impl]
  lazy val vertices : T => Seq[T] = circular("vertices")(Seq[T]()) { case node =>
    ((node +: children(node).flatMap(n => n -> vertices).toSeq) ++ children(node)).distinct // make the vertices unique
  }

  /** compute all the edges which compose this graph */
  private[impl]
  lazy val edges : T => Seq[(T, T)] = circular("edges")(Seq[(T, T)]()) { case node =>
    (children(node).map(n => node -> n) ++ children(node).flatMap(n => n -> edges)).distinct // make the edges unique
  }

  /** initialize the Kiama attributes but only if they haven't been set before */
  protected def initAttributable[A <: Attributable](a: A): A  = {
    if (a.children == null || !a.children.hasNext) {
      initTree(a)
    }
    a
  }

}


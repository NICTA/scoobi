/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
  lazy val parent : CachedAttribute[T, Option[T]] =
    attr("parent") { case node => Option(node.parent.asInstanceOf[T]) }

  /** compute the children of a node */
  lazy val children : CachedAttribute[T, Seq[T]] =
    attr("children") { case node => Vector(node.children.toSeq.map(_.asInstanceOf[T]):_*) }

  /** the root of the graph, computed from a given node */
  lazy val root : CachedAttribute[T, T] =
    attr("root") { case node =>
      parent(node).map(root).getOrElse(node)
    }
  
  /**
   * compute all the descendents of a node
   * They are all the recursive children reachable from this node
   */
  lazy val descendents : CachedAttribute[T, Seq[T]] = attr("descendents") { case node =>
    (children(node) ++ children(node).flatMap(descendents)).distinct
  }

  lazy val descendentsUntil: CachedParamAttribute[(T => Boolean), T, Seq[T]] = paramAttr("descendentsUntil") { (predicate: (T => Boolean)) => (node: T) =>
    children(node).filterNot(predicate) ++ children(node).filterNot(predicate).flatMap(descendentsUntil(predicate)).distinct
  }

  /** compute the parents of a node, that is all the chain of parents from this node up to the root of the graph */
  lazy val parents : CachedAttribute[T, Seq[T]] = attr("parents") { case node =>
    val p = parent(node).toSeq
    p ++ p.flatMap(parents)
  }

  /**
   * the nodes which have this node as a direct child
   *
   * for efficiency, this uses a table of all the nodes using a given one, computed from the root
   */
  lazy val uses : CachedAttribute[T, Set[T]] = attr("uses") { case node =>
    usesTable(node -> root).getOrElse(node, Set())
  }

  /** true if a node is used at most once */
  lazy val isUsedAtMostOnce : CachedAttribute[T, Boolean] =
    attr("isUsedAtMostOnce") { case node => uses(node).size <= 1 }

  /** a Map of all the nodes which are using a given node */
  private lazy val usesTable : CachedAttribute[T, Map[T, Set[T]]] = attr("usesTable") { case node =>
    Vector(children(node):_*).foldMap((child: T) => usesTable(child) |+| Map(child -> Set(node)))
  }

  /** compute all the indirect uses of a given node, that is all the nodes which have this node as a descendent */
  lazy val transitiveUses : CachedAttribute[T, Set[T]] = attr("transitiveUses") { case node =>
    (uses(node) ++ uses(node).flatMap(_ -> transitiveUses)).toSet
  }

  /** reinit usages */
  protected def reinitUses {
    Seq[CachedAttribute[_,_]](root, parent, parents, children, descendents, usesTable, uses, transitiveUses, isUsedAtMostOnce, isCyclic, vertices, edges).foreach(_.reset)
    Seq[CachedParamAttribute[_,_,_]](isParentOf, isStrictParentOf, descendentsUntil).foreach(_.reset)
  }

  /**
   * return true if a CompNode has a cycle in its graph,
   * this will be detected by Kiama throwing an exception when fetching the descendents of a node
   */
  lazy val isCyclic: CachedAttribute[T, Boolean] = attr((n: T) => tryKo(n -> descendents))

  /** @return true if 1 node is parent of the other, or if they are the same node */
  private[impl]
  lazy val isParentOf: CachedParamAttribute[T, T, Boolean] = paramAttr("isParentOf") { (other: T) => node: T =>
    (node -> isStrictParentOf(other)) || (node == other)
  }

  /** @return true if 1 node is parent of the other, or but not  the same node */
  private[impl]
  lazy val isStrictParentOf: CachedParamAttribute[T, T, Boolean] = paramAttr("isStrictParentOf") { (other: T) => node: T =>
    (node -> descendents).contains(other) || (other -> descendents).contains(node)
  }

  /** compute the vertices starting from a node */
  private[impl]
  lazy val vertices: CachedAttribute[T, Seq[T]] = attr("vertices") { case node =>
    ((node +: children(node).flatMap(n => n -> vertices).toSeq) ++ children(node)).distinct // make the vertices unique
  }

  /** compute all the edges which compose this graph */
  private[impl]
  lazy val edges: CachedAttribute[T, Seq[(T, T)]] = attr("edges") { case node =>
    (children(node).map(n => node -> n) ++ children(node).flatMap(n => n -> edges)).distinct // make the edges unique
  }

  /** initialise the Kiama attributes but only if they haven't been set before */
  def initAttributable[A <: Attributable](a: A): A  = {
    if (a.children == null || !a.children.hasNext) reinitAttributable(a)
    else                                           a
  }
  def reinitAttributable[A <: Attributable](a: A): A  = { initTree(a); a }

}


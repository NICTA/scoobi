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
import scalaz.std.vector.vectorSyntax._
import org.kiama.attribution.{Attributable, AttributionCore}
import control.Exceptions._
import scala.collection.GenTraversable
import scala.annotation.tailrec

/**
 * generic functions for a nodes graph
 */
trait GraphNodes extends AttributionCore {

  type T <: Attributable

  /**
   * compute the parent of a node. This relationship is actually maintained while getting the children of a node
   * if the parent node has not been set while recursing for children, then it is None
   */
  lazy val parent : CachedAttribute[T, Option[T]] =
    attr { case node => None }

  /**
   * compute the children of a node.
   *
   * This is similar to calling the initTree method on the node but this stores the information as an attribute instead of storing it
   * as a tree. This is a way to avoid conflicts if we have 2 processes trying to initialise the same graph
   */
  lazy val children : CachedAttribute[T, Seq[T]] =
    attr {
      case node => {
        val childNodes =
        node.productIterator.flatMap { child =>
          child match {
            case Some (o)              => setParent(Vector(o)         , node)
            case Left (l)              => setParent(Vector(l)         , node)
            case Right (r)             => setParent(Vector(r)         , node)
            case (a, b)                => setParent(Vector(a, b)      , node)
            case (a, b, c)             => setParent(Vector(a, b, c)   , node)
            case (a, b, c, d)          => setParent(Vector(a, b, c, d), node)
            case s : GenTraversable[_] => setParent(s.seq.toVector    , node)
            case a: Attributable       => setParent(Vector(a)         , node)
            case other                 => Vector()
          }
        }.map(_.asInstanceOf[T])
        childNodes.foldLeft(Vector[T]()) { _ :+ _ }
      }
    }

  /** while setting the children of a node, also set its parent */
  private def setParent(children: Seq[_], p: Any): Seq[_] =
    children.collect { case child: Attributable => parent.memo.put(child.asInstanceOf[T], Some(Some(p.asInstanceOf[T]))); child }

  /** the root of the graph, computed from a given node */
  lazy val root : CachedAttribute[T, T] =
    attr { case node =>
      parent(node).map(root).getOrElse(node)
    }
  
  /**
   * compute all the descendents of a node
   * They are all the recursive children reachable from this node
   */
  lazy val descendents : CachedAttribute[T, Seq[T]] = attr { case node =>
    getDescendents(children(node))
  }

  /**
   * This could be implemented as a recursive attribute but then
   * it might stack overflow
   */
  @tailrec
  private def getDescendents(nodes: Seq[T], result: Seq[T] = Vector()): Seq[T] = {
    if (nodes.isEmpty) result.distinct
    else if (nodes.size == 1) {
      val node = nodes.head
      if (children(node).isEmpty) result.distinct
      else getDescendents(children(node).drop(1), children(node)(0) +: result)
    } else {
      val node = nodes.head
      getDescendents(children(node) ++ nodes.drop(1), node +: result)
    }
  }

  lazy val descendentsUntil: CachedParamAttribute[(T => Boolean), T, Seq[T]] = paramAttr { (predicate: (T => Boolean)) => (node: T) =>
    children(node).filterNot(predicate) ++ children(node).filterNot(predicate).flatMap(descendentsUntil(predicate)).distinct
  }

  /** compute the parents of a node, that is all the chain of parents from this node up to the root of the graph */
  lazy val parents : CachedAttribute[T, Seq[T]] = attr { case node =>
    val p = parent(node).toSeq
    p ++ p.flatMap(parents)
  }

  /**
   * the nodes which have this node as a direct child
   *
   * for efficiency, this uses a table of all the nodes using a given one, computed from the root
   */
  lazy val uses : CachedAttribute[T, Set[T]] = attr { case node =>
    usesTable(node -> root).getOrElse(node, Set())
  }

  /** true if a node is used at most once */
  lazy val isUsedAtMostOnce : CachedAttribute[T, Boolean] =
    attr { case node => uses(node).size <= 1 }

  /** a Map of all the nodes which are using a given node */
  private lazy val usesTable : CachedAttribute[T, Map[T, Set[T]]] = attr { case node =>
    Vector(children(node):_*).foldMap((child: T) => usesTable(child) |+| Map(child -> Set(node)))
  }

  /** compute all the indirect uses of a given node, that is all the nodes which have this node as a descendent */
  lazy val transitiveUses : CachedAttribute[T, Set[T]] = attr { case node =>
    (uses(node) ++ uses(node).flatMap(_ -> transitiveUses)).toSet
  }

  /** reinit usages */
  protected def resetUses {
    Seq[CachedAttribute[_,_]](root, parents, descendents, usesTable, uses, transitiveUses, isUsedAtMostOnce, isCyclic, vertices, edges).foreach(_.reset)
    Seq[CachedParamAttribute[_,_,_]](isParentOf, isStrictParentOf, descendentsUntil).foreach(_.reset)
  }

  /**
   * return true if a CompNode has a cycle in its graph,
   * this will be detected by Kiama throwing an exception when fetching the descendents of a node
   */
  lazy val isCyclic: CachedAttribute[T, Boolean] = attr((n: T) => tryKo(n -> descendents))

  /** @return true if 1 node is parent of the other, or if they are the same node */
  private[impl]
  lazy val isParentOf: CachedParamAttribute[T, T, Boolean] = paramAttr { (other: T) => node: T =>
    (node -> isStrictParentOf(other)) || (node == other)
  }

  /** @return true if 1 node is parent of the other, or but not  the same node */
  private[impl]
  lazy val isStrictParentOf: CachedParamAttribute[T, T, Boolean] = paramAttr { (other: T) => node: T =>
    (node -> descendents).contains(other) || (other -> descendents).contains(node)
  }

  /** compute the vertices starting from a node */
  private[impl]
  lazy val vertices: CachedAttribute[T, Seq[T]] = attr { case node =>
    ((node +: children(node).flatMap(n => n -> vertices).toSeq) ++ children(node)).distinct // make the vertices unique
  }

  /** compute all the edges which compose this graph */
  private[impl]
  lazy val edges: CachedAttribute[T, Seq[(T, T)]] = attr { case node =>
    (children(node).map(n => node -> n) ++ children(node).flatMap(n => n -> edges)).distinct // make the edges unique
  }

  /** initialise the parent/child relationship recursively from node s */
  def initAttributable[S <: T](s: S): S  = s.synchronized {
    // recursive call for each child node
    children(s).foreach(initAttributable)
    s
  }

  /**
   * reinitialise all the attributes related to a node, starting from all the parent/children relationships
   *
   * reset the attributes, then recreate the parent/children relationships recursively
   */
  def reinit[S <: T](s: S): S  = s.synchronized {
    children.reset
    parent.reset
    resetUses
    initAttributable(s)
  }

}


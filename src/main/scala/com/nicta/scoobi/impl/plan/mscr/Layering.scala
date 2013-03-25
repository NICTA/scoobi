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
package mscr

import comp.ShowNode
import core.{UniqueInt, CompNode}

/**
 * Simple layering algorithm using the Longest path method to assign nodes to layers.
 *
 * See here for a good overview: http://www.cs.brown.edu/~rt/gdhandbook/chapters/hierarchical.pdf
 *
 * In our case the layers have minimum height and possibly big width which is actually good if we run things in parallel
 */
trait Layering extends ShowNode {

  type T <: CompNode

  /** a function to select only some nodes in the graph. They must be of type T */
  def selectNode: CompNode => Boolean

  lazy val selected: CompNode => Boolean = attr("selected node") { case n => selectNode(n) }
  lazy val select: PartialFunction[CompNode, T] = { case n if n -> selected => n.asInstanceOf[T] }

  lazy val selectedDescendents: CompNode => Seq[T] = attr("selected descendents") { case n =>
    (n -> descendents).collect(select)
  }

  /** @return the layer that a selected node is in. None if this is not a selected node */
  lazy val layer: CompNode => Option[Layer[T]] = attr("layer") { case n =>
    layers(root(n)).find(_.nodes.contains(n))
  }

  lazy val layers: CompNode => Seq[Layer[T]] = attr("layers") { case n =>
    val (leaves, nonLeaves) = selectedDescendents(n).partition { d =>
      selectedDescendents(d).isEmpty
    }
    val leaf = if (leaves.isEmpty && selectNode(n)) Seq(select(n)) else Seq()
    val result = Layer(leaves ++ leaf) +:
      nonLeaves.groupBy(_ -> longestPathSizeTo(leaves)).toSeq.sortBy(_._1).map { case (k, v) => Layer(v) }
    result.filterNot(_.isEmpty)
  }

  lazy val longestPathSizeTo: Seq[CompNode] => CompNode => Int = paramAttr("longestPathSizeToNodeFromSomeNodes") { (target: Seq[CompNode]) => node: CompNode =>
    target.map(t => node -> longestPathSizeToNode(t)).max
  }

  lazy val longestPathSizeToNode: CompNode => CompNode => Int = paramAttr("longestPathSizeToNodeFromOneNode") { (target: CompNode) => node: CompNode =>
    longestPathToNode(target)(node).size
  }

  lazy val longestPathToNode: CompNode => CompNode => Seq[CompNode] = paramAttr("longestPathToNodeFromOneNode") { (target: CompNode) => node: CompNode =>
    if (node.id == target.id)        Seq(node)  // found
    else if (children(node).isEmpty) Seq()      // not found
    else                             node +: children(node).map(_ -> longestPathToNode(target)).maxBy(_.size)
  }

  lazy val shortestPathToNode: CompNode => CompNode => Seq[CompNode] = paramAttr("shortestPathToNodeFromOneNode") { (target: CompNode) => node: CompNode =>
    if (node.id == target.id)        Seq(node)  // found
    else if (children(node).isEmpty) Seq()      // not found
    else                             node +: children(node).map(_ -> longestPathToNode(target)).minBy(_.size)
  }

  lazy val pathsToNode: CompNode => CompNode => Seq[Seq[CompNode]] = paramAttr("all the paths from one node to another") { (target: CompNode) => node: CompNode =>
    if (node.id == target.id)        Seq(Seq(node))  // found
    else if (children(node).isEmpty) Seq()           // not found
    else                             children(node).flatMap(ch => (ch -> pathsToNode(target)).filterNot(_.isEmpty).map(p => node +: p))
  }

  import Layer._
  /**
   * A layer contains group by keys and floating nodes defining mscrs so that none of them have dependencies relationship
   *
   * Because of this property they can be executed in parallel
   */
  case class Layer[T <: CompNode](nodes: Seq[T] = Seq[T]()) {
    val id = rollingInt.get
    lazy val gbks = nodes.collect(isAGroupByKey)

    lazy val isEmpty = nodes.isEmpty
    override def toString = nodes.mkString("Layer("+id+"\n  ", ",\n  ", ")\n")
  }

  object Layer {
    object rollingInt extends UniqueInt
  }

}


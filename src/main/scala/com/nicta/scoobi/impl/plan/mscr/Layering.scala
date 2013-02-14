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
  def selectNode(n: CompNode): Boolean

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
      nonLeaves.groupBy(_ -> longestPathTo(leaves)).toSeq.sortBy(_._1).map { case (k, v) => Layer(v) }
    result.filterNot(_.isEmpty)
  }

  lazy val longestPathTo: Seq[CompNode] => CompNode => Int = paramAttr("longestPathToNodeFromSomeNodes") { (target: Seq[CompNode]) => node: CompNode =>
    target.map(t => node -> longestPathToNode(t)).max
  }

  lazy val longestPathToNode: CompNode => CompNode => Int = paramAttr("longestPathToNodeFromOneNode") { (target: CompNode) => node: CompNode =>
    if (node.id == target.id)        0  // found
    else if (children(node).isEmpty) -1 // not found
    else                             1 + children(node).map(_ -> longestPathToNode(target)).max
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


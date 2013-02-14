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

import org.kiama.output.PrettyPrinter
import scalaz.Show
import com.github.mdr.ascii.layout._
import core._
import text.Showx._
import control.Exceptions._

/**
 * This trait contains some display functions for computation graphs
 */
trait ShowNode extends CompNodes {
  val prettyPrinter = new PrettyPrinter {}
  import prettyPrinter._

  /** @return a nested text representation of the nodes graph */
  def pretty(node : CompNode) = prettyPrinter.pretty(show(node))

  /**
   * show a node and recursively all its children
   */
  private def show(node: CompNode): Doc =
    node match {
      case Load1(_)              => value(showNode(node))
      case pd @ ParallelDo1(ins) => showNode(node) <> braces (nest (line <> "+" <> ssep (ins.map(i => show(i)), line <> "+")) <> line <> "env. " <> show(pd.env))
      case Return1(_)            => value(showNode(node))
      case Combine1(in)          => showNode(node) <> braces (nest (line <> show(in) <> line))
      case GroupByKey1(in)       => showNode(node) <> braces (nest (line <> show(in) <> line))
      case Materialise1(in)      => showNode(node) <> braces (nest (line <> show(in) <> line))
      case Op1(in1, in2)         => showNode(node) <> braces (nest (line <> "1. " <> show(in1) <> line <> "2. " <> show(in2)))
      case Root(ins)             => showNode(node) <> braces (nest (line <> "+" <> ssep (ins.map(i => show(i)), line <> "+")))
    }

  /** show a single node */
  private def showNode(n: CompNode) = n.toString


  /** show the structure without the ids or type annotations */
  lazy val showStructure = (n: CompNode) => pretty(n).
    replaceAll("\\d", "").             // remove ids
    replaceAll("bridge\\s*\\w*", "").  // remove bridge ids
    replaceAll("\\[[^\\s]+\\]", "")    // remove type annotations
  
  /**
   * Show instance for a CompNode
   */
  implicit lazy val showCompNode: Show[CompNode] = new Show[CompNode] {
    override def shows(node: CompNode) = pretty(node)
  }
  /**
   * Show is not covariant so it is necessary to add this implicit to prove that we can show subclasses of CompNode
   */
  implicit def showCompNodeInstance[T <: CompNode]: Show[T] = new Show[T] {
    override def show(t: T) = implicitly[Show[CompNode]].show(t)
  }

  /** @return a nested text representation of the nodes graph + graph if it's not too big */
  def prettyGraph = (node : CompNode) => pretty(node) + "\nGraph\n" + showGraph(node)

  /** @return an ASCII representation of the nodes graph */
  def showGraph(node : CompNode) = tryOrElse {
    if (descendents(node).size <= 50) {
      val graph = Graph((node -> vertices).toList.map(v => showNode(v)), (node -> edges).toList.map { case (v1, v2) => showNode(v1) -> showNode(v2) })
      Layouter.renderGraph(graph)
    } else "cannot represent the node as a graph because it is too big\n"
  }("cannot represent the node as a graph "+(if (isCyclic(node)) "(because there is a cycle)" else ""))

}

object ShowNode extends ShowNode
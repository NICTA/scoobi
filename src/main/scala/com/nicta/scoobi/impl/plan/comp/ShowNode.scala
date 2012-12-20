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
import mscr.MscrsDefinition

trait ShowNodeMscr extends ShowNode with MscrsDefinition

trait ShowNode extends CompNodes {
  val prettyPrinter = new PrettyPrinter {}
  import prettyPrinter._

  /** show the structure without the ids or type annotations */
  lazy val showStructure = (n: CompNode) => pretty(n).
    replaceAll("\\d", "").          // remove ids
    replaceAll("\\[[^\\s]+\\]", "") // remove type annotations
  /**
   * Show instance for a CompNode
   */
  implicit lazy val showCompNode: Show[CompNode] = new Show[CompNode] {
    override def shows(n: CompNode) = (n.toString + (n match {
      case Op1(in1, in2)    => Seq(in1, in2).showString("[","]")
      case Materialize1(in) => parens(pretty(in))
      case GroupByKey1(in)  => parens(pretty(in))
      case Combine1(in)     => parens(pretty(in))
      case ParallelDo1(ins) => ins.showString("[","]")
      case Load1(_)         => ""
      case Return1(_)       => ""
    }))
  }
  /**
   * Show is not covariant so it is necessary to add this implicit to prove that we can show subclasses of CompNode
   */
  implicit def showCompNodeInstance[T <: CompNode]: Show[T] = new Show[T] {
    override def show(t: T) = implicitly[Show[CompNode]].show(t)
  }

  /**
   * Pretty printer for a CompNode
   */
  def showNode[T](n: CompNode, attribute: Option[CompNode => T]) = n.toString + attribute.map(a => " -> "+ a(n).toString + " ").getOrElse("")
  lazy val show: CompNode => String = pretty(_:CompNode)

  /** @return a nested text representation of the nodes graph */
  def pretty(node : CompNode) = prettyPrinter.pretty(show(node, None))
  /** @return a nested text representation of the nodes graph + an attribute for each node */
  def pretty[T](node : CompNode, attribute: CompNode => T) = prettyPrinter.pretty(show(node, Some(attribute)))

  /** @return an ASCII representation of the nodes graph */
  def showGraph(node : CompNode) = tryOrElse {
    val graph = Graph((node -> vertices).toList.map(v => showNode(v, None)), (node -> edges).toList.map { case (v1, v2) => showNode(v1, None) -> showNode(v2, None) })
    Layouter.renderGraph(graph)
  }("cannot represent the node "+(if (isCyclic(node)) "(because there is a cycle)" else "")+"\n"+show(node))

  /** @return an ASCII representation of the nodes graph + an attribute for each node */
  def showGraph[T](node: CompNode, attribute: CompNode => T) = tryOrElse {
    // attributes are computed once here otherwise they will be recomputed when evaluating edges and the graph layout procedure will fail
    // because some edges extremities are not contained in the vertices set
    val attributedVertices = (node -> vertices).map(v => (v, showNode(v, Some(attribute)))).toMap
    val graph = Graph(attributedVertices.values.toList, (node -> edges).toList.map { case (v1, v2) => attributedVertices(v1) -> attributedVertices(v2) })
    Layouter.renderGraph(graph)
  }("cannot represent the attribute for "+show(node))

  /**
   * show a node and recursively all its children with a given attribute
   */
  private def show[T](node: CompNode, attribute: Option[CompNode => T] = None): Doc =
    node match {
      case Load1(_)              => value(showNode(node, attribute))
      case pd @ ParallelDo1(ins) => showNode(node, attribute) <> braces (nest (line <> "+" <> ssep (ins.map(i => show(i, attribute)), line <> "+")) <> line <> "env. " <> show(pd.env, attribute))
      case Return1(_)            => value(showNode(node, attribute))
      case Combine1(in)          => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
      case GroupByKey1(in)       => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
      case Materialize1(in)      => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
      case Op1(in1, in2)         => showNode(node, attribute) <> braces (nest (line <> "1. " <> show(in1, attribute) <> line <> "2. " <> show(in2, attribute)))
      case other                 => value(other)
    }
}
object ShowNode extends ShowNode
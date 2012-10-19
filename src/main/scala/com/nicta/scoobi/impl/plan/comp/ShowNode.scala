package com.nicta.scoobi
package impl
package plan
package comp

import scalaz.Show
import impl.text.Showx._
import org.kiama.output.PrettyPrinter
import graph.MscrGraph
import com.github.mdr.ascii.layout._
import control.Exceptions._

trait ShowNode extends MscrGraph with CompNodes{
  val prettyPrinter = new PrettyPrinter {}
  import prettyPrinter._

  /** show the structure without the ids */
  lazy val showStructure = (n: CompNode) => pretty(n).replaceAll("\\d", "")
  /**
   * Show instance for a CompNode
   */
  implicit lazy val showCompNode: Show[CompNode] = new Show[CompNode] {
    def show(n: CompNode) = (n.toString + (n match {
      case Op1(in1, in2)    => Seq(in1, in2).showString("[","]")
      case Flatten1(ins)    => ins.showString("[","]")
      case Materialize1(in) => parens(pretty(in))
      case GroupByKey1(in)  => parens(pretty(in))
      case Combine1(in)     => parens(pretty(in))
      case ParallelDo1(in)  => parens(pretty(in))
      case Load(_)          => ""
      case Return1(_)       => ""
    })).toList
  }
  /**
   * Show is not covariant so it is necessary to add this implicit to prove that we can show subclasses of CompNode
   */
  implicit def showCompNodeInstance[T <: CompNode]: Show[T] = new Show[T] {
    def show(t: T) = implicitly[Show[CompNode]].show(t)
  }

  /**
   * Pretty printer for a CompNode
   */
  def showNode[T](n: CompNode, attribute: Option[CompNode => T]) = n.toString + attribute.map(a => " -> "+ a(n).toString + " ").getOrElse("")
  lazy val show: CompNode => String = pretty(_:CompNode)
  /**
   * @return an ASCII representation of the nodes graph and their mscr
   *          The mscr of each node is only displayed if it is complete
   */
  def mscrsGraph[T](node: CompNode) = {
    def showMscr(n: CompNode) = {
      val m = n -> mscr
      n.toString +
        (if (m.isEmpty) "" else (" -> "+m))
    }
    val attributedVertices = (node -> vertices).map(v => (v, showMscr(v))).toMap
    tryOrElse {
      val graph = Graph(attributedVertices.values.toList, (node -> edges).toList.map { case (v1, v2) => attributedVertices(v1) -> attributedVertices(v2) }.distinct)
      Layouter.renderGraph(graph)
    }("cannot represent the Mscr for\n"+show(node))
  }

  /** @return a nested text representation of the nodes graph */
  def pretty(node : CompNode) = prettyPrinter.pretty(show(node, None))
  /** @return a nested text representation of the nodes graph + an attribute for each node */
  def pretty[T](node : CompNode, attribute: CompNode => T) = prettyPrinter.pretty(show(node, Some(attribute)))

  /** @return an ASCII representation of the nodes graph */
  def showGraph(node : CompNode) = tryOrElse {
    val graph = Graph((node -> vertices).toList.map(v => showNode(v, None)), (node -> edges).toList.map { case (v1, v2) => showNode(v1, None) -> showNode(v2, None) })
    Layouter.renderGraph(graph)
  }("cannot represent the node\n"+show(node))

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
      case Load(_)          => value(showNode(node, attribute))
      case Flatten1(ins)    => showNode(node, attribute) <> braces (nest (line <> "+" <> ssep (ins.map(i => show(i, attribute)), line <> "+")) <> line)
      case ParallelDo1(in)  => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
      case Return1(_)       => value(showNode(node, attribute))
      case Combine1(in)     => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
      case GroupByKey1(in)  => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
      case Materialize1(in) => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
      case Op1(in1,in2)     => showNode(node, attribute) <> braces (nest (line <> "1." <> show(in1, attribute) <> line <> "2." <> show(in2, attribute)))
    }
}
object ShowNode extends ShowNode
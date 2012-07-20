package com.nicta.scoobi
package impl
package plan

import graph.MscrGraph
import scalaz.Show
import org.kiama.output.PrettyPrinter

package object comp {

  import impl.util.UniqueInt

  object Id extends UniqueInt

  type CopyFn[A, Sh <: Shape] = (DComp[A, Sh], CopyTable) => (DComp[A, Sh], CopyTable, Boolean)
  type CopyTable = Map[DComp[_, _ <: Shape], DComp[_, _ <: Shape]]


  import text.Showx._

  /**
   * Show instance for a CompNode
   */
  implicit lazy val showCompNode: Show[CompNode] = new Show[CompNode] {
    def show(n: CompNode) = (n.toString + (n match {
      case Op(in1, in2, _)        => Seq(in1, in2).showString("[","]")
      case Flatten(ins)           => ins.showString("[","]")
      case Materialize(in)        => parens(in)
      case GroupByKey(in)         => parens(in)
      case Combine(in, _)         => parens(in)
      case ParallelDo(in,_,_,_,_) => parens(in)
      case Load(_)                => ""
      case Return(_)              => ""
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
  lazy val show = CompNodePrettyPrinter.pretty(_:CompNode): String

  import com.github.mdr.ascii.layout._
  import MscrGraph._

  /**
   * This pretty printer can use ASCII graphs or nested text to display CompNodes
   */
  object CompNodePrettyPrinter extends PrettyPrinter {
    /** @return a nested text representation of the nodes graph */
    def pretty(node : CompNode) = super.pretty(show(node))
    /** @return a nested text representation of the nodes graph + an attribute for each node */
    def pretty[T](node : CompNode, attribute: CompNode => T) = super.pretty(show(node, Some(attribute)))

    /** @return an ASCII representation of the nodes graph */
    def graph(node : CompNode) = {
      val graph = Graph((node -> vertices).toList.map(v => showNode(v, None)), (node -> edges).toList.map { case (v1, v2) => showNode(v1, None) -> showNode(v2, None) })
      Layouter.renderGraph(graph)
    }

    /** @return an ASCII representation of the nodes graph + an attribute for each node */
    def graph[T](node: CompNode, attribute: CompNode => T) = {
      // attributes are computed once here otherwise they will be recomputed when evaluating edges and the graph layout procedure will fail
      // because some edges extremities are not contained in the vertices set
      val attributedVertices = (node -> vertices).map(v => (v, showNode(v, Some(attribute)))).toMap
      val graph = Graph(attributedVertices.values.toList, (node -> edges).toList.map { case (v1, v2) => attributedVertices(v1) -> attributedVertices(v2) })
      Layouter.renderGraph(graph)
    }

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
      val graph = Graph(attributedVertices.values.toList, (node -> edges).toList.map { case (v1, v2) => attributedVertices(v1) -> attributedVertices(v2) }.distinct)
      Layouter.renderGraph(graph)
    }

    /**
     * show a node and recursively all its children with a given attribute
     */
    def show[T](node: CompNode, attribute: Option[CompNode => T] = None): Doc =
      node match {
        case Load(_)                => value(showNode(node, attribute))
        case Flatten(ins)           => showNode(node, attribute) <> braces (nest (line <> "+" <> ssep (ins.map(i => show(i, attribute)), line <> "+")) <> line)
        case ParallelDo(in,_,_,_,_) => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
        case Return(_)              => value(showNode(node, attribute))
        case Combine(in,_)          => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
        case GroupByKey(in)         => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
        case Materialize(in)        => showNode(node, attribute) <> braces (nest (line <> show(in, attribute) <> line))
        case Op(in1, in2, _)        => showNode(node, attribute) <> braces (nest (line <> "1." <> show(in1, attribute) <> line <> "2." <> show(in2, attribute)))
      }
  }
}
package com.nicta.scoobi
package impl
package plan

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
  lazy val showNode = (_:CompNode).toString
  lazy val show = CompNodePrettyPrinter.pretty(_:CompNode): String

  object CompNodePrettyPrinter extends PrettyPrinter {
    def pretty(node : CompNode) = super.pretty(show(node))

    def show(node : CompNode): Doc =
      node match {
        case Load(_)                => value(showNode(node))
        case Flatten(ins)           => showNode(node) <> braces (nest (line <> "+" <> ssep (ins map show, line <> "+")) <> line)
        case ParallelDo(in,_,_,_,_) => showNode(node) <> braces (nest (line <> show(in) <> line))
        case Return(_)              => value(showNode(node))
        case Combine(in,_)          => showNode(node) <> braces (nest (line <> show(in) <> line))
        case GroupByKey(in)         => showNode(node) <> braces (nest (line <> show(in) <> line))
        case Materialize(in)        => showNode(node) <> braces (nest (line <> show(in) <> line))
        case Op(in1, in2, _)        => showNode(node) <> braces (nest (line <> "1." <> show(in1) <> line <> "2." <> show(in2)))
      }
  }
}
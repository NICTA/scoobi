package com.nicta.scoobi
package impl
package plan

import scalaz.Show

package object comp {

  import impl.util.UniqueInt

  object Id extends UniqueInt

  type CopyFn[A, Sh <: Shape] = (DComp[A, Sh], CopyTable) => (DComp[A, Sh], CopyTable, Boolean)
  type CopyTable = Map[DComp[_, _ <: Shape], DComp[_, _ <: Shape]]


  import text.Showx._

  /**
   * Show instance for a CompNode
   */
  implicit def show[T <: CompNode]: Show[T] = new Show[T] {
    def show(n: T) = (n.toString + (n match {
      case Op(in1, in2, _)        => Seq(in1: CompNode, in2).showString("[","]")
      case Flatten(ins)           => ins.showString("[","]")
      case Materialize(in)        => parens(in)
      case GroupByKey(in)         => parens(in)
      case Combine(in, _)         => parens(in)
      case ParallelDo(in,_,_,_,_) => parens(in)
      case Load(_)                => ""
      case Return(_)              => ""
    })).toList
  }

}
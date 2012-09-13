package com.nicta.scoobi
package impl
package plan

import text.Showx._
import graph.MscrGraph
import scalaz.Show
import org.kiama.output.PrettyPrinter

package object comp {

  import impl.util.UniqueInt

  object Id extends UniqueInt

  type CopyFn[A, Sh <: Shape] = (DComp[A, Sh], CopyTable) => (DComp[A, Sh], CopyTable, Boolean)
  type CopyTable = Map[DComp[_, _ <: Shape], DComp[_, _ <: Shape]]
}
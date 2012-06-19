package com.nicta.scoobi
package impl
package plan
package smart

import core.{Grouping, WireFormat}
import exec.Env


/** The Materialize node type specifies the conversion of an Arr DComp to an Exp DComp. */
case class Materialize[A](in: DComp[A, Arr]) extends DComp[Iterable[A], Exp] {
  override val toString = "Materialize ("+id+")"
  val toVerboseString = toString + "(" + in.toVerboseString + ")"
}

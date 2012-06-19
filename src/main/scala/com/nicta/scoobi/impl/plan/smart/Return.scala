package com.nicta.scoobi
package impl
package plan
package smart

import core.{Grouping, WireFormat}
import exec.Env

/** The Return node type specifies the building of a Exp DComp from an "ordinary" value. */
case class Return[A](x: A) extends DComp[A, Exp] {

  override val toString = "Return ("+id+")"
  val toVerboseString = toString + "(" + x.toString + ")"
}



package com.nicta.scoobi
package impl
package plan
package smart

import core.{Grouping, WireFormat}
import exec.Env


/** The Op node type specifies the building of Exp DComp by applying a function to the values
 * of two other Exp DComp nodes. */
case class Op[A, B, C](in1: DComp[A, Exp], in2: DComp[B, Exp], f: (A, B) => C) extends DComp[C, Exp] {

  override val toString = "Op" + id
  val toVerboseString = toString + "[" + in1.toVerboseString + "," + in2.toVerboseString + "]"
}


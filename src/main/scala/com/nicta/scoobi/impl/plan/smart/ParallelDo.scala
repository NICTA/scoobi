package com.nicta.scoobi
package impl
package plan
package smart

import core.{Grouping, Emitter, WireFormat, EnvDoFn}
import impl.exec.TaggedIdentityMapper

/** The ParallelDo node type specifies the building of a DComp as a result of applying a function to
 * all elements of an existing DComp and concatenating the results. */
case class ParallelDo[A, B, E](in: DComp[A, Arr], env: DComp[E, Exp], dofn: EnvDoFn[A, B, E], groupBarrier: Boolean = false, fuseBarrier: Boolean = false) extends DComp[B, Arr] {

  override val toString = "ParallelDo" + id + (if (groupBarrier) "*" else "") + (if (fuseBarrier) "%" else "")
  val toVerboseString = toString + "(" + env.toVerboseString + "," + in.toVerboseString + ")"
}

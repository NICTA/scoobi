package com.nicta.scoobi
package impl
package plan
package smart

import core.{Emitter, EnvDoFn, Grouping, WireFormat}
import exec.TaggedIdentityMapper


/** The Combine node type specifies the building of a DComp as a result of applying an associative
 * function to the values of an existing key-values DComp. */
case class Combine[K, V](in: DComp[(K, Iterable[V]), Arr], f: (V, V) => V) extends DComp[(K, V), Arr] {

  override val toString = "Combine" + id
  val toVerboseString = toString + "(" + in.toVerboseString + ")"

}


package com.nicta.scoobi
package impl
package plan
package smart

import core.{Grouping, WireFormat}
import exec.TaggedIdentityMapper
import org.kiama.attribution.Attributable

/** The Flatten node type spcecifies the building of a DComp that contains all the elements from
 * one or more exsiting DLists of the same type. */
case class Flatten[A](ins: List[DComp[A, Arr]]) extends DComp[A, Arr] {
  override val toString = "Flatten" + id
  val toVerboseString = toString + "([" + ins.map(_.toVerboseString).mkString(",") + "])"

  override def clone: Attributable = copy()

}

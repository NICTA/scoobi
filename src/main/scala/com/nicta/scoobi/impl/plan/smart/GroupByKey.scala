package com.nicta.scoobi
package impl
package plan
package smart

import core.{Grouping, WireFormat}
import exec.TaggedIdentityMapper
import org.kiama.attribution.Attributable


/** The GroupByKey node type specifies the building of a DComp as a result of partitioning an exiting
 * key-value DComp by key. */
case class GroupByKey[K, V](in: DComp[(K, V), Arr]) extends DComp[(K, Iterable[V]), Arr] {

  override val toString = "GroupByKey" + id
  val toVerboseString = toString + "(" + in.toVerboseString + ")"

  override def clone: Attributable = copy()

}

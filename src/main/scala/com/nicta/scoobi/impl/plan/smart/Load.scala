package com.nicta.scoobi
package impl
package plan
package smart

import core.{Grouping, WireFormat}
import io.DataSource
import exec.TaggedIdentityMapper
import plan.Smart.ConvertInfo

/** The Load node type specifies the creation of a DComp from some source other than another DComp.
 * A DataSource object specifies how the loading is performed. */
case class Load[A](source: DataSource[_, _, A]) extends DComp[A, Arr] {

  override val toString = "Load" + id
  val toVerboseString = toString
}


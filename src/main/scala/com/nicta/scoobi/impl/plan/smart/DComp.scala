package com.nicta.scoobi
package impl
package plan
package smart

import core.{Grouping, WireFormat}
import io.DataSource
import org.kiama.attribution.Attributable

/** GADT for distributed list computation graph. */
abstract class DComp[+A, +Sh <: Shape] extends AstNode

trait AstNode extends Attributable {
  lazy val id = Id.get
  def toVerboseString: String
}

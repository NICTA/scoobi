package com.nicta.scoobi
package testing
package script

import org.specs2.specification._
import script.Specification

abstract class UnitSpecification extends Specification {
  override def map(fs: =>Fragments) = section("unit") ^ super.map(fs)
}


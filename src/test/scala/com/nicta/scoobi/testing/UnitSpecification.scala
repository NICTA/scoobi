package com.nicta.scoobi
package testing

import org.specs2.Specification
import org.specs2.specification.{Fragments, Tags}

trait UnitSpecification extends Specification with Tags {
  override def map(fs: =>Fragments) = section("unit") ^ fs
}

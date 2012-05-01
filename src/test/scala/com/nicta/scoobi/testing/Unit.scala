package com.nicta.scoobi.testing

import org.specs2.Specification
import org.specs2.specification.{Fragments, Tags}

trait Unit extends Specification with Tags {
  override def map(fs: =>Fragments) = section("unit") ^ fs
}

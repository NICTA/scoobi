package com.nicta.scoobi.testing

import org.specs2.mutable.Tags
import org.specs2.mutable.Specification

trait Unit extends Tags {
  this: Specification =>
  section("unit")
}

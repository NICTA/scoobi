package com.nicta.scoobi

import org.specs2.specification.Analysis
import testing.mutable.UnitSpecification

class DependenciesSpec extends UnitSpecification with Analysis {
  val application =
    layers(
    "testing lib",
    "application",
    "io")

  val implementation =
    layers(
      "exec      form",
      "plan.comp plan.mscr",
      "mapreducer",
      "reflect rtt time",
      "util control text collection monitor").withPrefix("impl")

  val core =
    layers("core")

  eg {

    Layers(application.layers    ++
           implementation.layers ++
           core.layers             ).withPrefix("com.nicta.scoobi") must beRespected

  }
}

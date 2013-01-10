package com.nicta.scoobi

import org.specs2.specification.Analysis
import testing.mutable.UnitSpecification

class DependenciesSpec extends UnitSpecification with Analysis {
  val application =
    layers(
    "testing lib",
    "application",
    "io io.seq")

  val implementation =
    layers(
      "exec form",
      "plan plan.source plan.comp plan.mscr mapreducer",
      "time reflect rtt",
      "util control text collection monitor").withPrefix("impl")

  val core =
    layers("core")

  eg {
    Layers(application.layers    ++
           implementation.layers ++
           core.layers             ).withPrefix("com.nicta.scoobi") must beRespected
  }
}

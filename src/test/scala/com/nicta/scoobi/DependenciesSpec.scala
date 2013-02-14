/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
      "exec",
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

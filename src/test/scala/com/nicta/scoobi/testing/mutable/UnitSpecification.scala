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
package testing
package mutable

import org.specs2.mutable.Tags
import org.specs2.mutable.Specification
import org.specs2.specification.Fragments
import application.HadoopLogFactory

abstract class UnitSpecification extends Specification with Tags {

  // to avoid warnings from the Configurations object
  HadoopLogFactory.setLogFactory(quiet = true)

  section("unit")
}

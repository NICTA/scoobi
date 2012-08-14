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
package impl
package collection

import testing.mutable.UnitSpecification
import scala.collection._
import Maps._

class MapsSpec extends UnitSpecification {

  "A mutable map can be updated with keys from another map and a partial function to select the new keys to be added" >> {
    val updated = mutable.Map(1 -> "1", 2 -> "2").updateWith(Map(3 -> "3", 4 -> "4")) {
      case (k, v) =>
        (k, "got: " + v)
    }
    updated must_== mutable.Map(1 -> "1", 2 -> "2", 3 -> "got: 3", 4 -> "got: 4")
  }

}

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

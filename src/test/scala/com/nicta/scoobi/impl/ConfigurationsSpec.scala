package com.nicta.scoobi.impl

import org.specs2.mutable.Specification
import com.nicta.scoobi.impl.Configurations._

class ConfigurationsSpec extends Specification {
  "A configuration can be updated with the keys existing in another configuration" >> {
    val updated = configuration("a" -> "1", "b" -> "2").updateWith(configuration("c" -> "3", "d" -> "4")) {
      case (k, v) => ("got: "+k, "and: "+v)
    }
    updated.toMap must_== configuration("a" -> "1", "b" -> "2", "got: c" -> "and: 3", "got: d" -> "and: 4").toMap
  }
}

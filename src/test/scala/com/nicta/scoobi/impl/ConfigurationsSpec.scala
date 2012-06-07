package com.nicta.scoobi
package impl

import org.apache.hadoop.conf.Configuration
import testing.mutable.UnitSpecification
import Configurations._
import org.specs2.matcher.Matcher

class ConfigurationsSpec extends UnitSpecification{

  "A configuration can be updated with the keys existing in another configuration" >> {
    val updated = configuration("a" -> "1", "b" -> "2").updateWith(configuration("c" -> "3", "d" -> "4")) {
      case (k, v) => ("got: "+k, "and: "+v)
    }
    updated.toMap must_== configuration("a" -> "1", "b" -> "2", "got: c" -> "and: 3", "got: d" -> "and: 4").toMap
  }

  "It is possible to increment the Int value of a configuration key" >> {
    "for a simple key" >> {
      val c1 = configuration("a" -> "1")
      c1.increment("b")
      c1 must beTheSameAs(configuration("a" -> "1", "b" -> "1"))

      val c2 = configuration("a" -> "1")
      c2.increment("a")
      c2 must beTheSameAs(configuration("a" -> "2"))
    }
  }

  def beTheSameAs(other: Configuration): Matcher[Configuration] = (c: Configuration) =>
    (c.show == other.show, c.show+"\n\nis not the same as\n\n"+other.show)
}

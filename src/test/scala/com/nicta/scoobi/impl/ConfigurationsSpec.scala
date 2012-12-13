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

  "Values can be added to a given key" >> {
    "they will be comma separated by default" >> {
      val c1 = configuration()
      c1.addValues("a", "1", "2").get("a") === "1,2"
     }
    "redundant values are suppressed" >> {
      val c1 = configuration("a" -> "1,2")
      c1.addValues("a", "2", "1", "3", "4").get("a") === "1,2,3,4"
    }
  }

  "A configuration has methods to access keys and values" >> {
    "'defines' returns true if a key is defined" >> {
      "a key is defined"     ==> (configuration("a" -> "1,2").defines("a") === true)
      "a key is not defined" ==> (configuration("b" -> "1,2").defines("a") === false)
    }
    "'getOrSet' returns an existing value or set a default one for a given key" >> {
      "the existing value was not overriden" ==> { configuration("a" -> "1").getOrSet("a", "3") === "1" }

      val c = configuration("a" -> "1")
      "the key doesn't exist" ==> { c.getOrSet("b", "3") === "3" }
      "the default value has been set" ==> { c.get("b") === "3" }
    }
  }

  def beTheSameAs(other: Configuration): Matcher[Configuration] = (c: Configuration) =>
    (c.show == other.show, c.show+"\n\nis not the same as\n\n"+other.show)
}

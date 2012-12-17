package com.nicta.scoobi
package impl
package util

import testing.mutable.UnitSpecification
import org.apache.hadoop.conf.Configuration
import java.io.ByteArrayOutputStream

class SerialiserSpec extends UnitSpecification {
  "it is possible to serialize a configuration object without its classloader" >> {
    val configuration = new Configuration
    val out = new ByteArrayOutputStream

    Serialiser.serialise(configuration, out)
    out.toString must not contain("classLoader")
  }
}

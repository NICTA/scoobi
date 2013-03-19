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
package util

import testing.mutable.UnitSpecification
import org.apache.hadoop.conf.Configuration
import java.io.{OutputStream, InputStream, ByteArrayInputStream, ByteArrayOutputStream}

class SerialiserSpec extends UnitSpecification {
  "it is possible to serialize a configuration object without its classloader" >> {
    serialise(new Configuration).toString must not contain("classLoader")
  }

  "a serialised object must not failed to be serialised even with unicode characters" >> {
    deserialise(serialise("abc\001")) === "abc\001"
  }

  def serialise(a: Any): ByteArrayOutputStream = {
    val out = new ByteArrayOutputStream
    Serialiser.serialise(a, out)
    out
  }

  def deserialise(in: ByteArrayOutputStream): Any = {
    val input = new ByteArrayInputStream(in.toByteArray)
    Serialiser.deserialise(input)
  }
}

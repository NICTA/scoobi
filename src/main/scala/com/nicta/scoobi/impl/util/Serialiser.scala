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

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.io.xml.StaxDriver
import org.apache.hadoop.conf.Configuration
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import core.ScoobiConfiguration

trait Serialiser {
  private val xstream = new XStream(new StaxDriver())
  xstream.omitField(classOf[Configuration], "classLoader")
  xstream.omitField(classOf[Configuration], "CACHE_CLASSES")
  xstream.omitField(classOf[ScoobiConfiguration], "sc")

  def serialise(obj: Any, out: OutputStream) {
    try { xstream.toXML(obj, out) }
    finally { out.close()  }
  }

  def deserialise(in: InputStream) = {
    xstream.fromXML(in)
  }

  def toByteArray(obj: Any) = {
    val out = new ByteArrayOutputStream
    serialise(obj, out)
    out.toByteArray
  }

  def fromByteArray(in: Array[Byte]) =
    deserialise(new ByteArrayInputStream(in))

}
object Serialiser extends Serialiser

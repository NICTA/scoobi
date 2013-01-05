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

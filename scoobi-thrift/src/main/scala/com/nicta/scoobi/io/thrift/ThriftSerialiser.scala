package com.nicta.scoobi.io.thrift

import org.apache.thrift.{TDeserializer, TSerializer}
import org.apache.thrift.protocol.TCompactProtocol

/**
 * Util for converting a `ThriftLike` object to and from bytes.
 *
 * WARNING: This class is _not_ threadsafe and should be used with extreme caution!
 *
 * https://issues.apache.org/jira/browse/THRIFT-2218
 */
case class ThriftSerialiser() {

  val serialiser = new TSerializer(new TCompactProtocol.Factory)
  val deserialiser = new TDeserializer(new TCompactProtocol.Factory)

  def toBytes[A](a: A)(implicit ev: A <:< ThriftLike): Array[Byte] =
    serialiser.serialize(ev(a))

  def fromBytes[A](empty: A, bytes: Array[Byte])(implicit ev: A <:< ThriftLike): A = {
    val e = ev(empty).deepCopy
    e.clear()
    deserialiser.deserialize(e, bytes)
    e.asInstanceOf[A]
  }
}

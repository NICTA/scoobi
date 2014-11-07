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
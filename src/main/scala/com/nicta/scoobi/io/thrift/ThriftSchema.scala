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

import java.io.{DataInput, DataOutput}

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.BytesWritable

/**
 * Schema for creating Thrift WireFormat and SeqSchema instances.
 */
object ThriftSchema {

  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  def mkThriftFmt[A](implicit m: Manifest[A], ev: A <:< ThriftLike): WireFormat[A] = new WireFormat[A] {
    // Call once when the implicit is created to avoid further reflection
    val empty = m.runtimeClass.newInstance().asInstanceOf[A]

    def toWire(x: A, out: DataOutput) = {
      val bytes = ThriftSerialiser().toBytes(x)
      out.writeInt(bytes.length)
      out.write(bytes)
    }

    def fromWire(in: DataInput): A = {
      val size = in.readInt()
      val bytes = new Array[Byte](size)
      in.readFully(bytes)
      ThriftSerialiser().fromBytes(empty, bytes)
    }

    override def toString = "ThriftObject"
  }

  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS*/
  def mkThriftSchema[A](implicit m: Manifest[A], ev: A <:< ThriftLike) = new SeqSchema[A] {
    type SeqType = BytesWritable

    // Call once when the implicit is created to avoid further reflection
    val empty = m.runtimeClass.newInstance().asInstanceOf[A]

    def toWritable(x: A) = new BytesWritable(ThriftSerialiser().toBytes(x))

    def fromWritable(x: BytesWritable): A = ThriftSerialiser().fromBytes(empty, x.getBytes)

    val mf: Manifest[SeqType] = implicitly
  }
}
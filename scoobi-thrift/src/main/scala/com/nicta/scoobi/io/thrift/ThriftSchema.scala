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

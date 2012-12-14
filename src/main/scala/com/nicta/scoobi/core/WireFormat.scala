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
package core

import annotation.implicitNotFound
import java.io._
import impl.slow
import org.apache.hadoop.io.Writable
import collection.generic.CanBuildFrom
import collection.mutable.{ListBuffer, Builder}
import org.apache.avro.io.EncoderFactory
import com.nicta.scoobi.io.avro.AvroSchema
import org.apache.avro.mapred.{AvroKey, AvroWrapper}
import org.apache.avro.hadoop.io.AvroSerialization
import org.apache.hadoop.io.serializer.{Serializer, Deserializer}
import org.apache.avro.reflect.ReflectDatumWriter
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificData, SpecificRecordBase}
import org.apache.avro.io.DecoderFactory
import collection.mutable.ArrayBuffer

/**Type-class for sending types across the Hadoop wire. */
@implicitNotFound(msg = "Cannot find WireFormat type class for ${A}")
trait WireFormat[A] {
  def toWire(x: A, out: DataOutput)
  def fromWire(in: DataInput): A
}

object WireFormat extends WireFormatImplicits {

  // extend WireFormat with useful methods
  implicit def wireFormat[A](wf: WireFormat[A]): WireFormatX[A] = new WireFormatX[A](wf)

  case class WireFormatX[A](wf: WireFormat[A]) {
    /**
     * transform a WireFormat[A] to a WireFormat[B] by providing a bijection A <=> B
     */
    def adapt[B](f: B => A, g: A => B): WireFormat[B] = new WireFormat[B] {
      def fromWire(in: DataInput) = g(wf.fromWire(in))
      def toWire(x: B, out: DataOutput) { wf.toWire(f(x), out) }
    }
  }
}

/** Implicit definitions of WireFormat instances for common types. */
trait WireFormatImplicits extends codegen.GeneratedWireFormats {

  class ObjectWireFormat[T](val x: T) extends WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {}
    override def fromWire(in: DataInput): T = x
  }
  def mkObjectWireFormat[T](x: T): WireFormat[T] = new ObjectWireFormat(x)

  class Case0WireFormat[T](val apply: () => T, val unapply: T => Boolean) extends WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {}
    override def fromWire(in: DataInput): T = apply()
  }

  def mkCaseWireFormat[T](apply: () => T, unapply: T => Boolean): WireFormat[T] = new Case0WireFormat(apply, unapply)

  class Case1WireFormat[T, A1: WireFormat](apply: (A1) => T, unapply: T => Option[(A1)]) extends WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      implicitly[WireFormat[A1]].toWire(unapply(obj).get, out)
    }
    override def fromWire(in: DataInput): T = apply(implicitly[WireFormat[A1]].fromWire(in))
  }
  def mkCaseWireFormat[T, A1: WireFormat](apply: (A1) => T, unapply: T => Option[(A1)]): WireFormat[T] = new Case1WireFormat(apply, unapply)

  /**
   * Catch-all implementation of a WireFormat for a type T, using Java serialization.
   * It is however very inefficient, so for production use, consider creating your own WireFormat instance.
   *
   * Note that this WireFormat instance definition is *not* implicit, meaning that you will need to explicitely
   * import it when you need it.
   */
  def AnythingFmt[T <: Serializable] = new WireFormat[T] {
    def toWire(x: T, out: DataOutput) {
      val bytesOut = new ByteArrayOutputStream
      val bOut = new ObjectOutputStream(bytesOut)
      bOut.writeObject(x)
      bOut.close()

      val arr = bytesOut.toByteArray
      out.writeInt(arr.size)
      out.write(arr)
    }

    def fromWire(in: DataInput): T = {
      val size = in.readInt()
      val barr = new Array[Byte](size)
      in.readFully(barr)

      val bIn = new ObjectInputStream(new ByteArrayInputStream(barr))
      bIn.readObject.asInstanceOf[T]
    }
  }

  /**
   * Hadoop Writable types.
   */
  implicit def WritableFmt[T <: Writable: Manifest] = new WireFormat[T] {
    def toWire(x: T, out: DataOutput) { x.write(out) }
    def fromWire(in: DataInput): T = {

      val x: T = try { implicitly[Manifest[T]].erasure.newInstance.asInstanceOf[T] } catch {
        case _: Throwable =>
          sys.error("Hadoop does not support using a Writable type (" +
            implicitly[Manifest[T]].erasure.getCanonicalName() +
            ") unless it can be constructed with an empty-argument constructor. One simple way to get around this," +
            " is by creating a new type by inheriting from, and making sure that subclass has a (working) no-argument constructor")
      }
      x.readFields(in)
      x
    }
  }
  
  /**
   * Avro types
   */
   implicit def AvroFmt[T <: SpecificRecordBase : Manifest : AvroSchema] = new AvroWireFormat[T]
  class AvroWireFormat[T <: SpecificRecordBase : Manifest : AvroSchema] extends WireFormat[T] {
    def toWire(x : T, out : DataOutput) {
      val sch = implicitly[AvroSchema[T]].schema
      val bytestream = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.directBinaryEncoder(bytestream, null)
      val writer : SpecificDatumWriter[T] = new SpecificDatumWriter(sch)
      writer.write(x,encoder)
      val outbytes = bytestream.toByteArray()
      out.writeInt(outbytes.size)
      out.write(outbytes)
    }
    def fromWire(in : DataInput) : T = {
      val sch = implicitly[AvroSchema[T]].schema
      val size = in.readInt
      val bytes : Array[Byte] = new Array[Byte](size)
      in.readFully(bytes)
      val decoder = DecoderFactory.get.directBinaryDecoder(new ByteArrayInputStream(bytes), null)
      val reader : SpecificDatumReader[T] = new SpecificDatumReader(sch)
      reader.read(null.asInstanceOf[T], decoder)
    }
  }


  /**
   * "Primitive" types.
   */
  implicit def UnitFmt = new WireFormat[Unit] {
    def toWire(x: Unit, out: DataOutput) {}
    def fromWire(in: DataInput): Unit = ()
  }

  implicit def IntFmt = new WireFormat[Int] {
    def toWire(x: Int, out: DataOutput) { out.writeInt(x) }
    def fromWire(in: DataInput): Int = in.readInt()
  }

  implicit def IntegerFmt: WireFormat[java.lang.Integer] = new WireFormat[java.lang.Integer] {
    def toWire(x: java.lang.Integer, out: DataOutput) { out.writeInt(x) }
    def fromWire(in: DataInput): java.lang.Integer = in.readInt()
  }

  implicit def BooleanFmt = new WireFormat[Boolean] {
    def toWire(x: Boolean, out: DataOutput) { out.writeBoolean(x) }
    def fromWire(in: DataInput): Boolean = in.readBoolean()
  }

  implicit def LongFmt = new WireFormat[Long] {
    def toWire(x: Long, out: DataOutput) { out.writeLong(x) }
    def fromWire(in: DataInput): Long = in.readLong()
  }

  implicit def DoubleFmt = new WireFormat[Double] {
    def toWire(x: Double, out: DataOutput) { out.writeDouble(x) }
    def fromWire(in: DataInput): Double = { in.readDouble() }
  }

  implicit def FloatFmt = new WireFormat[Float] {
    def toWire(x: Float, out: DataOutput) { out.writeFloat(x) }
    def fromWire(in: DataInput): Float = { in.readFloat() }
  }

  implicit def CharFmt = new WireFormat[Char] {
    def toWire(x: Char, out: DataOutput) { out.writeChar(x) }
    def fromWire(in: DataInput): Char = in.readChar()
  }

  implicit def ByteFmt = new WireFormat[Byte] {
    def toWire(x: Byte, out: DataOutput) { out.writeByte(x) }
    def fromWire(in: DataInput): Byte = in.readByte()
  }

  implicit def StringFmt: WireFormat[String] = new WireFormat[String] {
    def toWire(x: String, out: DataOutput) {
      if (x == null)
        out.writeInt(-1)
      else {
        val b = x.getBytes("utf-8")
        out.writeInt(b.length)
        out.write(b)
      }
    }
    def fromWire(in: DataInput): String = {
      val l = in.readInt()
      if (l == -1)
        null
      else {
        val b = new Array[Byte](l)
        in.readFully(b, 0, l)
        new String(b, "utf-8")
      }
    }
  }

  /*
   * Useful Scala types.
   */

  /**
   * Traversable structures
   */
  implicit def TraversableFmt[CC[X] <: Traversable[X], T](implicit wt: WireFormat[T], bf: CanBuildFrom[_, T, CC[T]]): WireFormat[CC[T]] =
    new TraversableWireFormat(bf())

  /**
   * Map structures
   */
  implicit def MapFmt[CC[X, Y] <: Map[X, Y], K, V](implicit wtK: WireFormat[K], wtV: WireFormat[V], bf: CanBuildFrom[_, (K, V), CC[K, V]]): WireFormat[CC[K, V]] =
    new TraversableWireFormat(bf())

  /* Arrays */
  implicit def ArrayFmt[T](implicit m: Manifest[T], wt: WireFormat[T]): WireFormat[Array[T]] =
    new TraversableWireFormat(new ListBuffer[T]) adapt ((a: Array[T]) => a.toList, (s: List[T]) => s.toArray)

  /**
   * This class is used to create a WireFormat for Traversables, Maps and Arrays
   */
  private[scoobi] case class TraversableWireFormat[T: WireFormat, CC <: Traversable[T]](builder: Builder[T, CC]) extends WireFormat[CC] {
    def toWire(x: CC, out: DataOutput) = {
      require(x != null, "Cannot serialise a null Traversable. Consider using an empty collection, or an Option[Traversable]")
      // The "naive" approach for persisting a Traversable would be to persist the number of elements, then the
      // elements themselves. However, if this Traversable is an Iterator, taking the size will consume the whole iterator
      // and the elements will not be serialised.
      //
      // So the strategy here is to only persist 1000 elements at the time, bringing them into memory to know their size
      // and iterate on them. We signal the end of all the "chunks" by persisting an empty iterator which will have a size
      // of 0
      (x.view.toSeq.sliding(1000, 1000).toSeq :+ Iterator.empty) foreach { chunk =>
        val elements = chunk.toSeq
        out.writeInt(elements.size)
        elements.foreach { implicitly[WireFormat[T]].toWire(_, out) }
      }
    }
    def fromWire(in: DataInput): CC = {
      var size = in.readInt()
      builder.clear()
      // when the size is 0, it means that we've reached the last "chunk" of elements
      while (size != 0) {
        (0 until size) foreach { _ => builder += implicitly[WireFormat[T]].fromWire(in) }
        size = in.readInt()
      }
      builder.result()
    }
  }

  /**
   * Option type.
   */
  implicit def OptionFmt[T](implicit wt: WireFormat[T]) = new WireFormat[Option[T]] {
    def toWire(x: Option[T], out: DataOutput) = x match {
      case Some(y) => { out.writeBoolean(true); wt.toWire(y, out) }
      case None => { out.writeBoolean(false) }
    }
    def fromWire(in: DataInput): Option[T] = {
      val isSome = in.readBoolean()
      if (isSome) {
        val x: T = wt.fromWire(in)
        Some(x)
      } else {
        None
      }
    }
  }

  /*
   * Either types.
   */
  implicit def EitherFmt[T1, T2](implicit wt1: WireFormat[T1], wt2: WireFormat[T2]) = new WireFormat[Either[T1, T2]] {
    def toWire(x: Either[T1, T2], out: DataOutput) = x match {
      case Left(x) => { out.writeBoolean(true); wt1.toWire(x, out) }
      case Right(x) => { out.writeBoolean(false); wt2.toWire(x, out) }
    }
    def fromWire(in: DataInput): Either[T1, T2] = {
      val isLeft = in.readBoolean()
      if (isLeft) {
        val x: T1 = wt1.fromWire(in)
        Left(x)
      } else {
        val x: T2 = wt2.fromWire(in)
        Right(x)
      }
    }
  }

  implicit def LeftFmt[T1, T2](implicit wt1: WireFormat[T1]) = new WireFormat[Left[T1, T2]] {
    def toWire(x: Left[T1, T2], out: DataOutput) = wt1.toWire(x.a, out)
    def fromWire(in: DataInput): Left[T1, T2] = Left[T1, T2](wt1.fromWire(in))
  }

  implicit def RightFmt[T1, T2](implicit wt1: WireFormat[T2]) = new WireFormat[Right[T1, T2]] {
    def toWire(x: Right[T1, T2], out: DataOutput) = wt1.toWire(x.b, out)
    def fromWire(in: DataInput): Right[T1, T2] = Right[T1, T2](wt1.fromWire(in))
  }

  /**
   * Java's Date
   */
  implicit def DateFmt = new WireFormat[java.util.Date] {
    def toWire(x: java.util.Date, out: DataOutput) = out.writeLong(x.getTime)
    def fromWire(in: DataInput): java.util.Date = new java.util.Date(in.readLong())
  }

  /*
   * Shapeless tagged types.
   */
  import shapeless.TypeOperators._
  implicit def taggedTypeWireFormat[T : WireFormat, U]: WireFormat[T @@ U] =
    implicitly[WireFormat[T]].asInstanceOf[WireFormat[T @@ U]]
}

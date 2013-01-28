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

import java.io._
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
import scalaz._, Scalaz._

import annotation.implicitNotFound
import collection.mutable

/** Typeclass for sending types across the Hadoop wire */
@implicitNotFound(msg = "Cannot find WireFormat type class for ${A}")
trait WireFormat[A] extends WireReaderWriter { outer =>

  def write(x: Any, out: DataOutput) { toWire(x.asInstanceOf[A], out) }
  def read(in: DataInput) = fromWire(in)

  def toWire(x: A, out: DataOutput)
  def fromWire(in: DataInput): A

  /**
   * Map a pair of functions on this exponential functor to produce a new wire format.
   */
  def xmap[B](f: A => B, g: B => A): WireFormat[B] =
    new WireFormat[B] {
      def toWire(x: B, out: DataOutput) =
        outer.toWire(g(x), out)

      def fromWire(in: DataInput) =
        f(outer.fromWire(in))

      override def toString = outer.toString+"->B"
    }

  /**
   * Produce a wire format on products from this wire format and the given wire format. Synonym for `***`.
   */
  def product[B](b: WireFormat[B]): WireFormat[(A, B)] = {
    implicit val WA: WireFormat[A] = this
    implicit val WB: WireFormat[B] = b
    implicitly[WireFormat[(A, B)]]
  }

  /**
   * Produce a wire format on products from this wire format and the given wire format. Synonym for `product`.
   */
  def ***[B](b: WireFormat[B]): WireFormat[(A, B)] =
    product(b)
}

trait WireReaderWriter { this: WireFormat[_] =>
  def write(a: Any, out: DataOutput)
  def read(in: DataInput): Any
}

object WireReaderWriter {
  /** Performs a deep copy of an arbitrary object by first serialising then deserialising
    * it via its WireFormat. */
  def wireReaderWriterCopy(a: Any)(wf: WireReaderWriter): Any = {
    import java.io._
    val byteArrOs = new ByteArrayOutputStream()
    wf.write(a, new DataOutputStream(byteArrOs))
    wf.read(new DataInputStream(new ByteArrayInputStream(byteArrOs.toByteArray)))
  }

}

object WireFormat extends WireFormatImplicits {
  def pair(wf1: WireReaderWriter, wf2: WireReaderWriter): WireReaderWriter =
    Tuple2Fmt(wf1.asInstanceOf[WireFormat[Any]], wf2.asInstanceOf[WireFormat[Any]])

  def iterable(wf: WireReaderWriter): WireReaderWriter =
    TraversableFmt(wf.asInstanceOf[WireFormat[Any]], implicitly[CanBuildFrom[_, Any, Iterable[_]]])

  /** Performs a deep copy of an arbitrary object by first serialising then deserialising
    * it via its WireFormat. */
  def wireFormatCopy[A : WireFormat](a: A): A = {
    import java.io._
    val byteArrOs = new ByteArrayOutputStream()
    wireFormat[A].toWire(a, new DataOutputStream(byteArrOs))
    wireFormat[A].fromWire(new DataInputStream(new ByteArrayInputStream(byteArrOs.toByteArray())))
  }

  def manifest[A](implicit mf: Manifest[A]): Manifest[A] = mf
  def grouping[A](implicit gp: Grouping[A]): Grouping[A] = gp
  def wireFormat[A](implicit wf: WireFormat[A]): WireFormat[A] = wf

}

/** Implicit definitions of WireFormat instances for common types. */
trait WireFormatImplicits extends codegen.GeneratedWireFormats {

  class ObjectWireFormat[T : Manifest](val x: T) extends WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {}
    override def fromWire(in: DataInput): T = x
    override def toString = implicitly[Manifest[T]].erasure.getSimpleName
  }
  def mkObjectWireFormat[T : Manifest](x: T): WireFormat[T] = new ObjectWireFormat(x)

  class Case0WireFormat[T : Manifest](val apply: () => T, val unapply: T => Boolean) extends WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {}
    override def fromWire(in: DataInput): T = apply()
    override def toString = implicitly[Manifest[T]].erasure.getSimpleName
  }

  def mkCaseWireFormat[T : Manifest](apply: () => T, unapply: T => Boolean): WireFormat[T] = new Case0WireFormat(apply, unapply)

  class Case1WireFormat[T, A1: WireFormat](apply: (A1) => T, unapply: T => Option[(A1)]) extends WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      implicitly[WireFormat[A1]].toWire(unapply(obj).get, out)
    }
    override def fromWire(in: DataInput): T = apply(implicitly[WireFormat[A1]].fromWire(in))
    override def toString = implicitly[WireFormat[A1]].toString
  }

  def mkCaseWireFormat[T, A1: WireFormat](apply: (A1) => T, unapply: T => Option[(A1)]): WireFormat[T] = new Case1WireFormat(apply, unapply)

  /**
   * Catch-all implementation of a WireFormat for a type T, using Java serialization.
   * It is however very inefficient, so for production use, consider creating your own WireFormat instance.
   *
   * Note that this WireFormat instance definition is *not* implicit, meaning that you will need to explicitely
   * import it when you need it.
   */
  def AnythingFmt[T <: Serializable]: WireFormat[T] = new AnythingWireFormat[T]
  class AnythingWireFormat[T <: Serializable] extends WireFormat[T] {
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
    override def toString = "Any"
  }

  /**
   * Hadoop Writable types.
   */
  implicit def WritableFmt[T <: Writable : Manifest] = new WireFormat[T] {
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
    override def toString = "Writable["+implicitly[Manifest[T]].erasure.getSimpleName+"]"
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
    override def toString = "Avro["+implicitly[Manifest[T]].erasure.getSimpleName+"]"
  }


  /**
   * "Primitive" types.
   */
  implicit def UnitFmt = new UnitWireFormat
  class UnitWireFormat extends WireFormat[Unit] {
    def toWire(x: Unit, out: DataOutput) {}
    def fromWire(in: DataInput): Unit = ()
    override def toString = "Unit"
  }

  implicit def IntFmt = new IntWireFormat
  class IntWireFormat extends WireFormat[Int] {
    def toWire(x: Int, out: DataOutput) { out.writeInt(x) }
    def fromWire(in: DataInput): Int = in.readInt()
    override def toString = "Int"
  }

  implicit def IntegerFmt: WireFormat[java.lang.Integer] = new IntegerWireFormat
  class IntegerWireFormat extends WireFormat[java.lang.Integer] {
    def toWire(x: java.lang.Integer, out: DataOutput) { out.writeInt(x) }
    def fromWire(in: DataInput): java.lang.Integer = in.readInt()
    override def toString = "Integer"
  }

  implicit def BooleanFmt = new BooleanWireFormat
  class BooleanWireFormat extends WireFormat[Boolean] {
    def toWire(x: Boolean, out: DataOutput) { out.writeBoolean(x) }
    def fromWire(in: DataInput): Boolean = in.readBoolean()
    override def toString = "Boolean"
  }

  implicit def LongFmt = new LongWireFormat
  class LongWireFormat extends WireFormat[Long] {
    def toWire(x: Long, out: DataOutput) { out.writeLong(x) }
    def fromWire(in: DataInput): Long = in.readLong()
    override def toString = "Long"
  }

  implicit def DoubleFmt = new DoubleWireFormat
  class DoubleWireFormat extends WireFormat[Double] {
    def toWire(x: Double, out: DataOutput) { out.writeDouble(x) }
    def fromWire(in: DataInput): Double = { in.readDouble() }
    override def toString = "Float"
  }

  implicit def FloatFmt = new FloatWireFormat
  class FloatWireFormat extends WireFormat[Float] {
    def toWire(x: Float, out: DataOutput) { out.writeFloat(x) }
    def fromWire(in: DataInput): Float = { in.readFloat() }
    override def toString = "Float"
  }

  implicit def CharFmt = new CharWireFormat
  class CharWireFormat extends WireFormat[Char] {
    def toWire(x: Char, out: DataOutput) { out.writeChar(x) }
    def fromWire(in: DataInput): Char = in.readChar()
    override def toString = "Char"
  }

  implicit def ByteFmt = new ByteWireFormat
  class ByteWireFormat extends WireFormat[Byte] {
    def toWire(x: Byte, out: DataOutput) { out.writeByte(x) }
    def fromWire(in: DataInput): Byte = in.readByte()
    override def toString = "Byte"
  }

  implicit def StringFmt: WireFormat[String] = new StringWireFormat
  class StringWireFormat extends WireFormat[String] {
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
    override def toString = "String"
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
    new TraversableWireFormat(new ListBuffer[T]) xmap ((s: List[T]) => s.toArray, (a: Array[T]) => a.toList)

  /**
   * This class is used to create a WireFormat for Traversables, Maps and Arrays
   */
  private[scoobi] case class TraversableWireFormat[T : WireFormat, CC <: Traversable[T]](builder: Builder[T, CC]) extends WireFormat[CC] {
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

    override def toString = "Traversable["+implicitly[WireFormat[T]]+"]"
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
    override def toString = "Option["+wt+"]"
  }

  /*
   * Either types.
   */
  implicit def EitherFmt[T1, T2](implicit wt1: WireFormat[T1], wt2: WireFormat[T2]) = new EitherWireFormat[T1, T2]
  class EitherWireFormat[T1, T2](implicit wt1: WireFormat[T1], wt2: WireFormat[T2]) extends WireFormat[Either[T1, T2]] {
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
    override def toString = "Either["+wt1+","+wt2+"]"
  }

  /**
   * Java's Date
   */
  implicit def DateFmt = new WireFormat[java.util.Date] {
    def toWire(x: java.util.Date, out: DataOutput) = out.writeLong(x.getTime)
    def fromWire(in: DataInput): java.util.Date = new java.util.Date(in.readLong())
    override def toString = "Date"
  }

  /*
   * Shapeless tagged types.
   */
  import shapeless.TypeOperators._
  implicit def taggedTypeWireFormat[T : WireFormat, U]: WireFormat[T @@ U] =
    implicitly[WireFormat[T]].asInstanceOf[WireFormat[T @@ U]]
}

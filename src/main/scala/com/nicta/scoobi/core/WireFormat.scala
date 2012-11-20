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

import annotation.implicitNotFound

/**Type-class for sending types across the Hadoop wire. */
@implicitNotFound(msg = "Cannot find WireFormat type class for ${A}")
trait WireFormat[A] {
  def toWire(x: A, out: DataOutput)
  def fromWire(in: DataInput): A
}

case class ManifestWireFormat[A](mf: Manifest[A], wf: WireFormat[A]) {
  override def toString = wf.toString
  def isUnit = toString == "Unit" || toString == "(Unit,Unit)"
}

object ManifestWireFormat extends LowPriorityManifestWireFormat {

  implicit def pairManifestWireFormat[T1 : ManifestWireFormat,
                                      T2 : ManifestWireFormat]: ManifestWireFormat[(T1, T2)] = {
    implicit val ((mf1, wf1), (mf2, wf2)) = (
      WireFormat.decompose[T1],
      WireFormat.decompose[T2])
    ManifestWireFormat(manifest             [(T1, T2)],
                       WireFormat.wireFormat[(T1, T2)])
  }

  implicit def manifestWireFormat3[T1 : ManifestWireFormat,
                                   T2 : ManifestWireFormat,
                                   T3 : ManifestWireFormat]: ManifestWireFormat[(T1, T2, T3)] = {
    implicit val ((mf1, wf1), (mf2, wf2), (mf3, wf3)) = (
      WireFormat.decompose[T1],
      WireFormat.decompose[T2],
      WireFormat.decompose[T3])
    ManifestWireFormat(manifest             [(T1, T2, T3)],
                       WireFormat.wireFormat[(T1, T2, T3)])
  }

  implicit def manifestWireFormat4[T1 : ManifestWireFormat,
                                   T2 : ManifestWireFormat,
                                   T3 : ManifestWireFormat,
                                   T4 : ManifestWireFormat]: ManifestWireFormat[(T1, T2, T3, T4)] = {
    implicit val ((mf1, wf1), (mf2, wf2), (mf3, wf3), (mf4, wf4)) = (
      WireFormat.decompose[T1],
      WireFormat.decompose[T2],
      WireFormat.decompose[T3],
      WireFormat.decompose[T4])
    ManifestWireFormat(manifest             [(T1, T2, T3, T4)],
                       WireFormat.wireFormat[(T1, T2, T3, T4)])
  }

implicit def manifestWireFormat5[T1 : ManifestWireFormat,
                                   T2 : ManifestWireFormat,
                                   T3 : ManifestWireFormat,
                                   T4 : ManifestWireFormat,
                                   T5 : ManifestWireFormat]: ManifestWireFormat[(T1, T2, T3, T4, T5)] = {
  implicit val ((mf1, wf1), (mf2, wf2), (mf3, wf3), (mf4, wf4), (mf5, wf5)) = (
    WireFormat.decompose[T1],
    WireFormat.decompose[T2],
    WireFormat.decompose[T3],
    WireFormat.decompose[T4],
    WireFormat.decompose[T5])
  ManifestWireFormat(manifest             [(T1, T2, T3, T4, T5)],
                     WireFormat.wireFormat[(T1, T2, T3, T4, T5)])

}

  implicit def manifestWireFormat6[T1 : ManifestWireFormat,
                                   T2 : ManifestWireFormat,
                                   T3 : ManifestWireFormat,
                                   T4 : ManifestWireFormat,
                                   T5 : ManifestWireFormat,
                                   T6 : ManifestWireFormat]: ManifestWireFormat[(T1, T2, T3, T4, T5, T6)] = {
    implicit val ((mf1, wf1), (mf2, wf2), (mf3, wf3), (mf4, wf4), (mf5, wf5), (mf6, wf6)) = (
      WireFormat.decompose[T1],
      WireFormat.decompose[T2],
      WireFormat.decompose[T3],
      WireFormat.decompose[T4],
      WireFormat.decompose[T5],
      WireFormat.decompose[T6])
    ManifestWireFormat(manifest             [(T1, T2, T3, T4, T5, T6)],
                       WireFormat.wireFormat[(T1, T2, T3, T4, T5, T6)])
  }

  implicit def manifestWireFormat7[T1 : ManifestWireFormat,
                                   T2 : ManifestWireFormat,
                                   T3 : ManifestWireFormat,
                                   T4 : ManifestWireFormat,
                                   T5 : ManifestWireFormat,
                                   T6 : ManifestWireFormat,
                                   T7 : ManifestWireFormat]: ManifestWireFormat[(T1, T2, T3, T4, T5, T6, T7)] = {
    implicit val ((mf1, wf1), (mf2, wf2), (mf3, wf3), (mf4, wf4), (mf5, wf5), (mf6, wf6), (mf7, wf7)) = (
      WireFormat.decompose[T1],
      WireFormat.decompose[T2],
      WireFormat.decompose[T3],
      WireFormat.decompose[T4],
      WireFormat.decompose[T5],
      WireFormat.decompose[T6],
      WireFormat.decompose[T7])
    ManifestWireFormat(manifest             [(T1, T2, T3, T4, T5, T6, T7)],
                       WireFormat.wireFormat[(T1, T2, T3, T4, T5, T6, T7)])

  }


  implicit def manifestWireFormat8[T1 : ManifestWireFormat,
                                   T2 : ManifestWireFormat,
                                   T3 : ManifestWireFormat,
                                   T4 : ManifestWireFormat,
                                   T5 : ManifestWireFormat,
                                   T6 : ManifestWireFormat,
                                   T7 : ManifestWireFormat,
                                   T8 : ManifestWireFormat]: ManifestWireFormat[(T1, T2, T3, T4, T5, T6, T7, T8)] = {
    implicit val ((mf1, wf1), (mf2, wf2), (mf3, wf3), (mf4, wf4), (mf5, wf5), (mf6, wf6), (mf7, wf7), (mf8, wf8)) = (
      WireFormat.decompose[T1],
      WireFormat.decompose[T2],
      WireFormat.decompose[T3],
      WireFormat.decompose[T4],
      WireFormat.decompose[T5],
      WireFormat.decompose[T6],
      WireFormat.decompose[T7],
      WireFormat.decompose[T8])
    ManifestWireFormat(manifest             [(T1, T2, T3, T4, T5, T6, T7, T8)],
                       WireFormat.wireFormat[(T1, T2, T3, T4, T5, T6, T7, T8)])

  }


}

trait LowPriorityManifestWireFormat {
  implicit def manifestAndWireFormat[A](implicit mf: Manifest[A], wf: WireFormat[A]) =
    ManifestWireFormat(mf, wf)
}

object ManifestWireFormatDecompose {
  implicit def manifestWireFormatToManifest[A](implicit mwf: ManifestWireFormat[A]): Manifest[A] =
    mwf.mf

  implicit def manifestWireFormatToWireFormat[A](implicit mwf: ManifestWireFormat[A]): WireFormat[A] =
    mwf.wf
}

object WireFormat extends WireFormatImplicits {

  // extend WireFormat with useful methods
  implicit def extendedWireFormat[A](wf: WireFormat[A]): WireFormatX[A] = new WireFormatX[A](wf)

  case class WireFormatX[A](wf: WireFormat[A]) {
    /**
     * transform a WireFormat[A] to a WireFormat[B] by providing a bijection A <=> B
     */
    def adapt[B : Manifest](f: B => A, g: A => B): WireFormat[B] = new WireFormat[B] {
      def fromWire(in: DataInput) = g(wf.fromWire(in))
      def toWire(x: B, out: DataOutput) { wf.toWire(f(x), out) }
      override def toString = implicitly[Manifest[B]].erasure.getSimpleName
    }
    override def toString = wf.toString
  }

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
  def manifestWireFormat[A](implicit mwf: ManifestWireFormat[A]) = mwf
  def decompose[A](implicit mwf: ManifestWireFormat[A]) = (mwf.mf, mwf.wf)
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
    new TraversableWireFormat(new ListBuffer[T]) adapt ((a: Array[T]) => a.toList, (s: List[T]) => s.toArray)

  /**
   * This class is used to create a WireFormat for Traversables, Maps and Arrays
   */
  private[scoobi] case class TraversableWireFormat[T: WireFormat, CC <: Traversable[T]](builder: Builder[T, CC]) extends WireFormat[CC] {
    def toWire(x: CC, out: DataOutput) = {
      require(x != null, "Cannot serialize a null Traversable. Consider using an empty collection, or an Option[Traversable]")
      // The "naive" approach for persisting a Traversable would be to persist the number of elements, then the
      // elements themselves. However, if this Traversable is an Iterator, taking the size will consume the whole iterator
      // and the elements will not be serialized.
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
    override def toString = "Either["+wt1+","+wt2+"]"
  }

  implicit def LeftFmt[T1, T2](implicit wt1: WireFormat[T1]) = new WireFormat[Left[T1, T2]] {
    def toWire(x: Left[T1, T2], out: DataOutput) = wt1.toWire(x.a, out)
    def fromWire(in: DataInput): Left[T1, T2] = Left[T1, T2](wt1.fromWire(in))
    override def toString = "Left["+wt1+"]"
  }

  implicit def RightFmt[T1, T2](implicit wt1: WireFormat[T2]) = new WireFormat[Right[T1, T2]] {
    def toWire(x: Right[T1, T2], out: DataOutput) = wt1.toWire(x.b, out)
    def fromWire(in: DataInput): Right[T1, T2] = Right[T1, T2](wt1.fromWire(in))
    override def toString = "Right["+wt1+"]"
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

/**
  * Copyright 2011 National ICT Australia Limited
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

import java.io._
import org.apache.hadoop.io._
import annotation.implicitNotFound


/** Type-class for sending types across the Hadoop wire. */
@implicitNotFound(msg = "Cannot find HadoopWritable type class for ${A}")
trait HadoopWritable[A] extends Serializable {
  def toWire(x: A, out: DataOutput)
  def fromWire(in: DataInput): A
  def show(x: A): String
}


/** Implicit definitions of HadoopWritable instances for common types. */
object HadoopWritable {

    def allowWritable[T, A1: HadoopWritable](apply: (A1) => T, unapply: T => Option[(A1)]): HadoopWritable[T] = new HadoopWritable[T] {

    override def toWire(obj: T, out: DataOutput) {
      val v: A1 = unapply(obj).get

      implicitly[HadoopWritable[A1]].toWire(v, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[HadoopWritable[A1]].fromWire(in)
      apply(a1)
    }

    override def show(x: T): String = x.toString
  }

  def allowWritable[T, A1: HadoopWritable, A2: HadoopWritable](apply: (A1, A2) => T, unapply: T => Option[(A1, A2)]): HadoopWritable[T] = new HadoopWritable[T] {

    override def toWire(obj: T, out: DataOutput) {
      val v: Product2[A1, A2] = unapply(obj).get

      implicitly[HadoopWritable[A1]].toWire(v._1, out)
      implicitly[HadoopWritable[A2]].toWire(v._2, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[HadoopWritable[A1]].fromWire(in)
      val a2: A2 = implicitly[HadoopWritable[A2]].fromWire(in)
      apply(a1, a2)
    }

    override def show(x: T): String = x.toString
  }

  def allowWritable[T, A1: HadoopWritable, A2: HadoopWritable, A3: HadoopWritable](apply: (A1, A2, A3) => T, unapply: T => Option[(A1, A2, A3)]): HadoopWritable[T] = new HadoopWritable[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product3[A1, A2, A3] = unapply(obj).get
      implicitly[HadoopWritable[A1]].toWire(v._1, out)
      implicitly[HadoopWritable[A2]].toWire(v._2, out)
      implicitly[HadoopWritable[A3]].toWire(v._3, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[HadoopWritable[A1]].fromWire(in)
      val a2: A2 = implicitly[HadoopWritable[A2]].fromWire(in)
      val a3: A3 = implicitly[HadoopWritable[A3]].fromWire(in)
      apply(a1, a2, a3)
    }

    override def show(x: T): String = x.toString
  }

  def allowWritable[T, A1: HadoopWritable, A2: HadoopWritable, A3: HadoopWritable, A4: HadoopWritable](apply: (A1, A2, A3, A4) => T, unapply: T => Option[(A1, A2, A3, A4)]): HadoopWritable[T] = new HadoopWritable[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product4[A1, A2, A3, A4] = unapply(obj).get
      implicitly[HadoopWritable[A1]].toWire(v._1, out)
      implicitly[HadoopWritable[A2]].toWire(v._2, out)
      implicitly[HadoopWritable[A3]].toWire(v._3, out)
      implicitly[HadoopWritable[A4]].toWire(v._4, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[HadoopWritable[A1]].fromWire(in)
      val a2: A2 = implicitly[HadoopWritable[A2]].fromWire(in)
      val a3: A3 = implicitly[HadoopWritable[A3]].fromWire(in)
      val a4: A4 = implicitly[HadoopWritable[A4]].fromWire(in)
      apply(a1, a2, a3, a4)
    }

    override def show(x: T): String = x.toString
  }

  def allowWritable[T, A1: HadoopWritable, A2: HadoopWritable, A3: HadoopWritable, A4: HadoopWritable, A5: HadoopWritable](apply: (A1, A2, A3, A4, A5) => T, unapply: T => Option[(A1, A2, A3, A4, A5)]): HadoopWritable[T] = new HadoopWritable[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product5[A1, A2, A3, A4, A5] = unapply(obj).get
      implicitly[HadoopWritable[A1]].toWire(v._1, out)
      implicitly[HadoopWritable[A2]].toWire(v._2, out)
      implicitly[HadoopWritable[A3]].toWire(v._3, out)
      implicitly[HadoopWritable[A4]].toWire(v._4, out)
      implicitly[HadoopWritable[A5]].toWire(v._5, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[HadoopWritable[A1]].fromWire(in)
      val a2: A2 = implicitly[HadoopWritable[A2]].fromWire(in)
      val a3: A3 = implicitly[HadoopWritable[A3]].fromWire(in)
      val a4: A4 = implicitly[HadoopWritable[A4]].fromWire(in)
      val a5: A5 = implicitly[HadoopWritable[A5]].fromWire(in)
      apply(a1, a2, a3, a4, a5)
    }

    override def show(x: T): String = x.toString
  }

  def allowWritable[T, A1: HadoopWritable, A2: HadoopWritable, A3: HadoopWritable, A4: HadoopWritable, A5: HadoopWritable, A6: HadoopWritable](apply: (A1, A2, A3, A4, A5, A6) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6)]): HadoopWritable[T] = new HadoopWritable[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product6[A1, A2, A3, A4, A5, A6] = unapply(obj).get
      implicitly[HadoopWritable[A1]].toWire(v._1, out)
      implicitly[HadoopWritable[A2]].toWire(v._2, out)
      implicitly[HadoopWritable[A3]].toWire(v._3, out)
      implicitly[HadoopWritable[A4]].toWire(v._4, out)
      implicitly[HadoopWritable[A5]].toWire(v._5, out)
      implicitly[HadoopWritable[A6]].toWire(v._6, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[HadoopWritable[A1]].fromWire(in)
      val a2: A2 = implicitly[HadoopWritable[A2]].fromWire(in)
      val a3: A3 = implicitly[HadoopWritable[A3]].fromWire(in)
      val a4: A4 = implicitly[HadoopWritable[A4]].fromWire(in)
      val a5: A5 = implicitly[HadoopWritable[A5]].fromWire(in)
      val a6: A6 = implicitly[HadoopWritable[A6]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6)
    }

    override def show(x: T): String = x.toString
  }

  def allowWritable[T, A1: HadoopWritable, A2: HadoopWritable, A3: HadoopWritable, A4: HadoopWritable, A5: HadoopWritable, A6: HadoopWritable, A7: HadoopWritable](apply: (A1, A2, A3, A4, A5, A6, A7) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7)]): HadoopWritable[T] = new HadoopWritable[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product7[A1, A2, A3, A4, A5, A6, A7] = unapply(obj).get
      implicitly[HadoopWritable[A1]].toWire(v._1, out)
      implicitly[HadoopWritable[A2]].toWire(v._2, out)
      implicitly[HadoopWritable[A3]].toWire(v._3, out)
      implicitly[HadoopWritable[A4]].toWire(v._4, out)
      implicitly[HadoopWritable[A5]].toWire(v._5, out)
      implicitly[HadoopWritable[A6]].toWire(v._6, out)
      implicitly[HadoopWritable[A7]].toWire(v._7, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[HadoopWritable[A1]].fromWire(in)
      val a2: A2 = implicitly[HadoopWritable[A2]].fromWire(in)
      val a3: A3 = implicitly[HadoopWritable[A3]].fromWire(in)
      val a4: A4 = implicitly[HadoopWritable[A4]].fromWire(in)
      val a5: A5 = implicitly[HadoopWritable[A5]].fromWire(in)
      val a6: A6 = implicitly[HadoopWritable[A6]].fromWire(in)
      val a7: A7 = implicitly[HadoopWritable[A7]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7)
    }

    override def show(x: T): String = x.toString
  }

  def allowWritable[T, A1: HadoopWritable, A2: HadoopWritable, A3: HadoopWritable, A4: HadoopWritable, A5: HadoopWritable, A6: HadoopWritable, A7: HadoopWritable, A8: HadoopWritable](apply: (A1, A2, A3, A4, A5, A6, A7, A8) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8)]): HadoopWritable[T] = new HadoopWritable[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product8[A1, A2, A3, A4, A5, A6, A7, A8] = unapply(obj).get
      implicitly[HadoopWritable[A1]].toWire(v._1, out)
      implicitly[HadoopWritable[A2]].toWire(v._2, out)
      implicitly[HadoopWritable[A3]].toWire(v._3, out)
      implicitly[HadoopWritable[A4]].toWire(v._4, out)
      implicitly[HadoopWritable[A5]].toWire(v._5, out)
      implicitly[HadoopWritable[A6]].toWire(v._6, out)
      implicitly[HadoopWritable[A7]].toWire(v._7, out)
      implicitly[HadoopWritable[A8]].toWire(v._8, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[HadoopWritable[A1]].fromWire(in)
      val a2: A2 = implicitly[HadoopWritable[A2]].fromWire(in)
      val a3: A3 = implicitly[HadoopWritable[A3]].fromWire(in)
      val a4: A4 = implicitly[HadoopWritable[A4]].fromWire(in)
      val a5: A5 = implicitly[HadoopWritable[A5]].fromWire(in)
      val a6: A6 = implicitly[HadoopWritable[A6]].fromWire(in)
      val a7: A7 = implicitly[HadoopWritable[A7]].fromWire(in)
      val a8: A8 = implicitly[HadoopWritable[A8]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8)
    }

    override def show(x: T): String = x.toString
  }

  /*
   * Catch-all
   */
  implicit def Anything[T <: Serializable] = new HadoopWritable[T] {
    def toWire(x: T, out: DataOutput) = {
      val bytesOut = new ByteArrayOutputStream
      val bOut =  new ObjectOutputStream(bytesOut)
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

    def show(x: T) = x.toString
  }

  /*
   * Built-in Hadoop Writable types.
   */

  implicit def Int = new HadoopWritable[Int] {
    def toWire(x: Int, out: DataOutput) { out.writeInt(x) }
    def fromWire(in: DataInput): Int = in.readInt()
    def show(x: Int) = x.toString
  }

  implicit def Long = new HadoopWritable[Long] {
    def toWire(x: Long, out: DataOutput) { out.writeLong(x) }
    def fromWire(in: DataInput): Long = in.readLong()
    def show(x: Long) = x.toString
  }

  implicit def Double = new HadoopWritable[Double] {
    def toWire(x: Double, out: DataOutput) { out.writeDouble(x) }
    def fromWire(in: DataInput): Double = { in.readDouble() }
    def show(x: Double) = x.toString
  }

  implicit def Char = new HadoopWritable[Char] {
    def toWire(x: Char, out: DataOutput) { out.writeChar(x) }
    def fromWire(in: DataInput): Char = in.readChar()
    def show(x: Char) = x.toString
  }

  implicit def Byte = new HadoopWritable[Byte] {
    def toWire(x: Byte, out: DataOutput) { out.writeByte(x) }
    def fromWire(in: DataInput): Byte = in.readByte()
    def show(x: Byte) = x.toString
  }

  implicit def String = new HadoopWritable[String] {
    def toWire(x: String, out: DataOutput) {
      require(x != null, "Error, trying to serialize a null String. Consider using an empty string or Option[String]")
      out.writeUTF(x)
    }
    def fromWire(in: DataInput): String = in.readUTF()
    def show(x: String) = x.toString
  }

  /*
   * Useful Scala types.
   */
  implicit def Tuple2[T1, T2](implicit wt1: HadoopWritable[T1], wt2: HadoopWritable[T2]) =
    new HadoopWritable[(T1, T2)] {
      def toWire(x: (T1, T2), out: DataOutput) = {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
      }
      def fromWire(in: DataInput): (T1, T2) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        (a, b)
      }
      def show(x: (T1, T2)) = {
        val elems = List(wt1.show(x._1), wt2.show(x._2))
        elems.mkString("(", ",", ")")
      }
  }

  implicit def Tuple3[T1, T2, T3]
      (implicit wt1: HadoopWritable[T1], wt2: HadoopWritable[T2], wt3: HadoopWritable[T3]) =
    new HadoopWritable[(T1, T2, T3)] {
      def toWire(x: (T1, T2, T3), out: DataOutput) = {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        (a, b, c)
      }
      def show(x: (T1, T2, T3)) = {
        val elems = List(wt1.show(x._1), wt2.show(x._2), wt3.show(x._3))
        elems.mkString("(", ",", ")")
      }
  }

  implicit def Tuple4[T1, T2, T3, T4]
      (implicit wt1: HadoopWritable[T1], wt2: HadoopWritable[T2], wt3: HadoopWritable[T3], wt4: HadoopWritable[T4]) =
    new HadoopWritable[Tuple4[T1, T2, T3, T4]] {
      def toWire(x: (T1, T2, T3, T4), out: DataOutput) = {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        (a, b, c, d)
      }
      def show(x: (T1, T2, T3, T4)) = {
        val elems = List(wt1.show(x._1), wt2.show(x._2), wt3.show(x._3), wt4.show(x._4))
        elems.mkString("(", ",", ")")
      }
  }

  implicit def Tuple5[T1, T2, T3, T4, T5]
      (implicit wt1: HadoopWritable[T1], wt2: HadoopWritable[T2], wt3: HadoopWritable[T3], wt4: HadoopWritable[T4],
                wt5: HadoopWritable[T5]) =
    new HadoopWritable[(T1, T2, T3, T4, T5)] {
      def toWire(x: (T1, T2, T3, T4, T5), out: DataOutput) = {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        (a, b, c, d, e)
      }
      def show(x: (T1, T2, T3, T4, T5)) = {
        val elems = List(wt1.show(x._1), wt2.show(x._2), wt3.show(x._3), wt4.show(x._4), wt5.show(x._5))
        elems.mkString("(", ",", ")")
      }
  }

    implicit def Tuple6[T1, T2, T3, T4, T5, T6]
      (implicit wt1: HadoopWritable[T1], wt2: HadoopWritable[T2], wt3: HadoopWritable[T3], wt4: HadoopWritable[T4],
                wt5: HadoopWritable[T5], wt6: HadoopWritable[T6]) =
    new HadoopWritable[(T1, T2, T3, T4, T5, T6)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6), out: DataOutput) = {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        (a, b, c, d, e, f)
      }
      def show(x: (T1, T2, T3, T4, T5, T6)) = {
        val elems = List(wt1.show(x._1), wt2.show(x._2), wt3.show(x._3), wt4.show(x._4), wt5.show(x._5), wt6.show(x._6))
        elems.mkString("(", ",", ")")
      }
  }

  implicit def Tuple7[T1, T2, T3, T4, T5, T6, T7]
      (implicit wt1: HadoopWritable[T1], wt2: HadoopWritable[T2], wt3: HadoopWritable[T3], wt4: HadoopWritable[T4],
                wt5: HadoopWritable[T5], wt6: HadoopWritable[T6], wt7: HadoopWritable[T7]) =
    new HadoopWritable[(T1, T2, T3, T4, T5, T6, T7)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7), out: DataOutput) = {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        (a, b, c, d, e, f, g)
      }
      def show(x: (T1, T2, T3, T4, T5, T6, T7)) = {
        val elems = List(wt1.show(x._1), wt2.show(x._2), wt3.show(x._3), wt4.show(x._4), wt5.show(x._5), wt6.show(x._6), wt7.show(x._7))
        elems.mkString("(", ",", ")")
      }
  }

  implicit def Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]
      (implicit wt1: HadoopWritable[T1], wt2: HadoopWritable[T2], wt3: HadoopWritable[T3], wt4: HadoopWritable[T4],
                wt5: HadoopWritable[T5], wt6: HadoopWritable[T6], wt7: HadoopWritable[T7], wt8: HadoopWritable[T8]) =
    new HadoopWritable[(T1, T2, T3, T4, T5, T6, T7, T8)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8), out: DataOutput) = {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        (a, b, c, d, e, f, g, h)
      }
      def show(x: (T1, T2, T3, T4, T5, T6, T7, T8)) = {
        val elems = List(wt1.show(x._1), wt2.show(x._2), wt3.show(x._3), wt4.show(x._4), wt5.show(x._5), wt6.show(x._6), wt7.show(x._7), wt8.show(x._8))
        elems.mkString("(", ",", ")")
      }
  }

  /*
   * List-like structures
   */
  implicit def Iterable[T](implicit wt: HadoopWritable[T]) = new HadoopWritable[Iterable[T]] {
    def toWire(x: Iterable[T], out: DataOutput) = {
      require(x != null, "Cannot serialize a null Iterable. Consider using an empty collection, or a Option[Iterable]")
      out.writeInt(x.size)
      x.foreach { wt.toWire(_, out) }
    }
    def fromWire(in: DataInput): Iterable[T] = {
      import scala.collection.mutable._
      val size = in.readInt()
      val ml: MutableList[T] = new MutableList
      for (_ <- 0 to (size - 1)) { ml += wt.fromWire(in) }
      val il: List[T] = ml.toList
      il.toIterable
    }
    def show(x: Iterable[T]) = x.mkString("[", ",", "]")
  }

  /*
   * Option type.
   */
  implicit def Option[T](implicit wt: HadoopWritable[T]) = new HadoopWritable[Option[T]] {
    def toWire(x: Option[T], out: DataOutput) = x match {
      case Some(y) => { out.writeBoolean(true); wt.toWire(y, out) }
      case None    => { out.writeBoolean(false) }
    }
    def fromWire(in: DataInput): Option[T] = {
      val isSome = in.readBoolean()
      if (isSome) {
        val x: T = wt.fromWire(in)
        Some(x)
      }
      else {
        None
      }
    }
    def show(x: Option[T]) = x match {
      case Some(y) => "S{" + wt.show(y) + "}"
      case None    => "N{}"
    }
  }

  /*
   * Either types.
   */
  implicit def Either[T1, T2](implicit wt1: HadoopWritable[T1], wt2: HadoopWritable[T2]) = new HadoopWritable[Either[T1, T2]] {
    def toWire(x: Either[T1, T2], out: DataOutput) = x match {
      case Left(x)  => { out.writeBoolean(true); wt1.toWire(x, out) }
      case Right(x) => { out.writeBoolean(false); wt2.toWire(x, out) }
    }
    def fromWire(in: DataInput): Either[T1, T2] = {
      val isLeft = in.readBoolean()
      if (isLeft) {
        val x: T1 = wt1.fromWire(in)
        Left(x)
      }
      else {
        val x: T2 = wt2.fromWire(in)
        Right(x)
      }
    }
    def show(x: Either[T1, T2]) = x match {
      case Left(x)  => "L{" + wt1.show(x) + "}"
      case Right(x) => "R{" + wt2.show(x) + "}"
    }
  }
}

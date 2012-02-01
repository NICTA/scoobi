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
@implicitNotFound(msg = "Cannot find WireFormat type class for ${A}")
trait WireFormat[A] extends Serializable {
  def toWire(x: A, out: DataOutput)
  def fromWire(in: DataInput): A
  def show(x: A): String
}


/** Implicit definitions of WireFormat instances for common types. */
object WireFormat {
  def mkObjectWireFormat[T](x: T) = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {}

    override def fromWire(in: DataInput): T = {
      x
    }
    override def show(x: T) = x.toString
  }

  def mkCaseWireFormat[T](apply: () => T, unapply: T => Boolean): WireFormat[T] = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {}

    override def fromWire(in: DataInput): T = {
      apply()
    }

    override def show(x: T): String = x toString
  }

  def mkCaseWireFormat[T, A1: WireFormat](apply: (A1) => T, unapply: T => Option[(A1)]): WireFormat[T] = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {
      val v: A1 = unapply(obj).get

      implicitly[WireFormat[A1]].toWire(v, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[WireFormat[A1]].fromWire(in)
      apply(a1)
    }

    override def show(x: T): String = x.toString
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat](apply: (A1, A2) => T, unapply: T => Option[(A1, A2)]): WireFormat[T] = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {
      val v: Product2[A1, A2] = unapply(obj).get

      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[WireFormat[A1]].fromWire(in)
      val a2: A2 = implicitly[WireFormat[A2]].fromWire(in)
      apply(a1, a2)
    }

    override def show(x: T): String = x.toString
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat](apply: (A1, A2, A3) => T, unapply: T => Option[(A1, A2, A3)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product3[A1, A2, A3] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[WireFormat[A1]].fromWire(in)
      val a2: A2 = implicitly[WireFormat[A2]].fromWire(in)
      val a3: A3 = implicitly[WireFormat[A3]].fromWire(in)
      apply(a1, a2, a3)
    }

    override def show(x: T): String = x.toString
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat](apply: (A1, A2, A3, A4) => T, unapply: T => Option[(A1, A2, A3, A4)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product4[A1, A2, A3, A4] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[WireFormat[A1]].fromWire(in)
      val a2: A2 = implicitly[WireFormat[A2]].fromWire(in)
      val a3: A3 = implicitly[WireFormat[A3]].fromWire(in)
      val a4: A4 = implicitly[WireFormat[A4]].fromWire(in)
      apply(a1, a2, a3, a4)
    }

    override def show(x: T): String = x.toString
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat](apply: (A1, A2, A3, A4, A5) => T, unapply: T => Option[(A1, A2, A3, A4, A5)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product5[A1, A2, A3, A4, A5] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[WireFormat[A1]].fromWire(in)
      val a2: A2 = implicitly[WireFormat[A2]].fromWire(in)
      val a3: A3 = implicitly[WireFormat[A3]].fromWire(in)
      val a4: A4 = implicitly[WireFormat[A4]].fromWire(in)
      val a5: A5 = implicitly[WireFormat[A5]].fromWire(in)
      apply(a1, a2, a3, a4, a5)
    }

    override def show(x: T): String = x.toString
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat](apply: (A1, A2, A3, A4, A5, A6) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product6[A1, A2, A3, A4, A5, A6] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[WireFormat[A1]].fromWire(in)
      val a2: A2 = implicitly[WireFormat[A2]].fromWire(in)
      val a3: A3 = implicitly[WireFormat[A3]].fromWire(in)
      val a4: A4 = implicitly[WireFormat[A4]].fromWire(in)
      val a5: A5 = implicitly[WireFormat[A5]].fromWire(in)
      val a6: A6 = implicitly[WireFormat[A6]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6)
    }

    override def show(x: T): String = x.toString
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product7[A1, A2, A3, A4, A5, A6, A7] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[WireFormat[A1]].fromWire(in)
      val a2: A2 = implicitly[WireFormat[A2]].fromWire(in)
      val a3: A3 = implicitly[WireFormat[A3]].fromWire(in)
      val a4: A4 = implicitly[WireFormat[A4]].fromWire(in)
      val a5: A5 = implicitly[WireFormat[A5]].fromWire(in)
      val a6: A6 = implicitly[WireFormat[A6]].fromWire(in)
      val a7: A7 = implicitly[WireFormat[A7]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7)
    }

    override def show(x: T): String = x.toString
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product8[A1, A2, A3, A4, A5, A6, A7, A8] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
    }

    override def fromWire(in: DataInput): T = {
      val a1: A1 = implicitly[WireFormat[A1]].fromWire(in)
      val a2: A2 = implicitly[WireFormat[A2]].fromWire(in)
      val a3: A3 = implicitly[WireFormat[A3]].fromWire(in)
      val a4: A4 = implicitly[WireFormat[A4]].fromWire(in)
      val a5: A5 = implicitly[WireFormat[A5]].fromWire(in)
      val a6: A6 = implicitly[WireFormat[A6]].fromWire(in)
      val a7: A7 = implicitly[WireFormat[A7]].fromWire(in)
      val a8: A8 = implicitly[WireFormat[A8]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8)
    }

    override def show(x: T): String = x.toString
  }

  def mkAbstractWireFormat[T, A <: T : Manifest : WireFormat, B <: T : Manifest : WireFormat]() = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {
      val clazz: Class[_] = obj.getClass

      if (clazz == implicitly[Manifest[A]].erasure) {
        out.writeInt('A')
        implicitly[WireFormat[A]].toWire(obj.asInstanceOf[A], out)
      } else if (clazz == implicitly[Manifest[B]].erasure) {
        out.writeInt('B')
        implicitly[WireFormat[B]].toWire(obj.asInstanceOf[B], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): T =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }

    override def show(x: T) = x.toString
  }

  def mkAbstractWireFormat[T, A <: T : Manifest : WireFormat, B <: T : Manifest : WireFormat,  C <: T : Manifest : WireFormat]() = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {
      val clazz: Class[_] = obj.getClass

      if (clazz == implicitly[Manifest[A]].erasure) {
        out.writeInt('A')
        implicitly[WireFormat[A]].toWire(obj.asInstanceOf[A], out)
      } else if (clazz == implicitly[Manifest[B]].erasure) {
        out.writeInt('B')
        implicitly[WireFormat[B]].toWire(obj.asInstanceOf[B], out)
      } else if (clazz == implicitly[Manifest[C]].erasure) {
        out.writeInt('C')
        implicitly[WireFormat[C]].toWire(obj.asInstanceOf[C], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): T =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }

    override def show(x: T) = x.toString
  }

  def mkAbstractWireFormat[T, A <: T : Manifest : WireFormat, B <: T : Manifest : WireFormat,  C <: T : Manifest : WireFormat, D <: T : Manifest : WireFormat]() = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {
      val clazz: Class[_] = obj.getClass

      if (clazz == implicitly[Manifest[A]].erasure) {
        out.writeInt('A')
        implicitly[WireFormat[A]].toWire(obj.asInstanceOf[A], out)
      } else if (clazz == implicitly[Manifest[B]].erasure) {
        out.writeInt('B')
        implicitly[WireFormat[B]].toWire(obj.asInstanceOf[B], out)
      } else if (clazz == implicitly[Manifest[C]].erasure) {
        out.writeInt('C')
        implicitly[WireFormat[C]].toWire(obj.asInstanceOf[C], out)
      } else if (clazz == implicitly[Manifest[D]].erasure) {
        out.writeInt('D')
        implicitly[WireFormat[D]].toWire(obj.asInstanceOf[D], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): T =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }

    override def show(x: T) = x.toString
  }

  def mkAbstractWireFormat[T, A <: T : Manifest : WireFormat, B <: T : Manifest : WireFormat,  C <: T : Manifest : WireFormat, D <: T : Manifest : WireFormat, E <: T : Manifest : WireFormat]() = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {
      val clazz: Class[_] = obj.getClass

      if (clazz == implicitly[Manifest[A]].erasure) {
        out.writeInt('A')
        implicitly[WireFormat[A]].toWire(obj.asInstanceOf[A], out)
      } else if (clazz == implicitly[Manifest[B]].erasure) {
        out.writeInt('B')
        implicitly[WireFormat[B]].toWire(obj.asInstanceOf[B], out)
      } else if (clazz == implicitly[Manifest[C]].erasure) {
        out.writeInt('C')
        implicitly[WireFormat[C]].toWire(obj.asInstanceOf[C], out)
      } else if (clazz == implicitly[Manifest[D]].erasure) {
        out.writeInt('D')
        implicitly[WireFormat[D]].toWire(obj.asInstanceOf[D], out)
      } else if (clazz == implicitly[Manifest[E]].erasure) {
        out.writeInt('E')
        implicitly[WireFormat[E]].toWire(obj.asInstanceOf[E], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): T =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }

    override def show(x: T) = x.toString
  }

  def mkAbstractWireFormat[T, A <: T : Manifest : WireFormat, B <: T : Manifest : WireFormat,  C <: T : Manifest : WireFormat, D <: T : Manifest : WireFormat, E <: T : Manifest : WireFormat, F <: T : Manifest : WireFormat]() = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {
      val clazz: Class[_] = obj.getClass

      if (clazz == implicitly[Manifest[A]].erasure) {
        out.writeInt('A')
        implicitly[WireFormat[A]].toWire(obj.asInstanceOf[A], out)
      } else if (clazz == implicitly[Manifest[B]].erasure) {
        out.writeInt('B')
        implicitly[WireFormat[B]].toWire(obj.asInstanceOf[B], out)
      } else if (clazz == implicitly[Manifest[C]].erasure) {
        out.writeInt('C')
        implicitly[WireFormat[C]].toWire(obj.asInstanceOf[C], out)
      } else if (clazz == implicitly[Manifest[D]].erasure) {
        out.writeInt('D')
        implicitly[WireFormat[D]].toWire(obj.asInstanceOf[D], out)
      } else if (clazz == implicitly[Manifest[E]].erasure) {
        out.writeInt('E')
        implicitly[WireFormat[E]].toWire(obj.asInstanceOf[E], out)
      } else if (clazz == implicitly[Manifest[F]].erasure) {
        out.writeInt('F')
        implicitly[WireFormat[F]].toWire(obj.asInstanceOf[F], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): T =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }

    override def show(x: T) = x.toString
  }

  def mkAbstractWireFormat[T, A <: T : Manifest : WireFormat, B <: T : Manifest : WireFormat,  C <: T : Manifest : WireFormat, D <: T : Manifest : WireFormat, E <: T : Manifest : WireFormat, F <: T : Manifest : WireFormat, G <: T : Manifest : WireFormat]() = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {
      val clazz: Class[_] = obj.getClass

      if (clazz == implicitly[Manifest[A]].erasure) {
        out.writeInt('A')
        implicitly[WireFormat[A]].toWire(obj.asInstanceOf[A], out)
      } else if (clazz == implicitly[Manifest[B]].erasure) {
        out.writeInt('B')
        implicitly[WireFormat[B]].toWire(obj.asInstanceOf[B], out)
      } else if (clazz == implicitly[Manifest[C]].erasure) {
        out.writeInt('C')
        implicitly[WireFormat[C]].toWire(obj.asInstanceOf[C], out)
      } else if (clazz == implicitly[Manifest[D]].erasure) {
        out.writeInt('D')
        implicitly[WireFormat[D]].toWire(obj.asInstanceOf[D], out)
      } else if (clazz == implicitly[Manifest[E]].erasure) {
        out.writeInt('E')
        implicitly[WireFormat[E]].toWire(obj.asInstanceOf[E], out)
      } else if (clazz == implicitly[Manifest[F]].erasure) {
        out.writeInt('F')
        implicitly[WireFormat[F]].toWire(obj.asInstanceOf[F], out)
      } else if (clazz == implicitly[Manifest[G]].erasure) {
        out.writeInt('G')
        implicitly[WireFormat[G]].toWire(obj.asInstanceOf[G], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): T =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }

    override def show(x: T) = x.toString
  }

  def mkAbstractWireFormat[T, A <: T : Manifest : WireFormat, B <: T : Manifest : WireFormat,  C <: T : Manifest : WireFormat, D <: T : Manifest : WireFormat, E <: T : Manifest : WireFormat, F <: T : Manifest : WireFormat, G <: T : Manifest : WireFormat, H <: T : Manifest : WireFormat]() = new WireFormat[T] {

    override def toWire(obj: T, out: DataOutput) {
      val clazz: Class[_] = obj.getClass

      if (clazz == implicitly[Manifest[A]].erasure) {
        out.writeInt('A')
        implicitly[WireFormat[A]].toWire(obj.asInstanceOf[A], out)
      } else if (clazz == implicitly[Manifest[B]].erasure) {
        out.writeInt('B')
        implicitly[WireFormat[B]].toWire(obj.asInstanceOf[B], out)
      } else if (clazz == implicitly[Manifest[C]].erasure) {
        out.writeInt('C')
        implicitly[WireFormat[C]].toWire(obj.asInstanceOf[C], out)
      } else if (clazz == implicitly[Manifest[D]].erasure) {
        out.writeInt('D')
        implicitly[WireFormat[D]].toWire(obj.asInstanceOf[D], out)
      } else if (clazz == implicitly[Manifest[E]].erasure) {
        out.writeInt('E')
        implicitly[WireFormat[E]].toWire(obj.asInstanceOf[E], out)
      } else if (clazz == implicitly[Manifest[F]].erasure) {
        out.writeInt('F')
        implicitly[WireFormat[F]].toWire(obj.asInstanceOf[F], out)
      } else if (clazz == implicitly[Manifest[G]].erasure) {
        out.writeInt('G')
        implicitly[WireFormat[G]].toWire(obj.asInstanceOf[G], out)
      } else if (clazz == implicitly[Manifest[H]].erasure) {
        out.writeInt('H')
        implicitly[WireFormat[H]].toWire(obj.asInstanceOf[H], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): T =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }

    override def show(x: T) = x.toString
  }

  /*
   * Catch-all
   */
  implicit def AnythingFmt[T <: Serializable] = new WireFormat[T] {
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
   * Hadoop Writable types.
   */
  implicit def WritableFmt[T <: Writable : Manifest] = new WireFormat[T] {
    def toWire(x: T, out: DataOutput) = x.write(out)
    def fromWire(in: DataInput): T = {
      val x: T = implicitly[Manifest[T]].erasure.newInstance.asInstanceOf[T]
      x.readFields(in)
      x
    }
    def show(x: T) = x.toString
  }


  /*
   * "Primitive" types.
   */
  implicit def IntFmt = new WireFormat[Int] {
    def toWire(x: Int, out: DataOutput) { out.writeInt(x) }
    def fromWire(in: DataInput): Int = in.readInt()
    def show(x: Int) = x.toString
  }

  implicit def IntegerFmt = new WireFormat[java.lang.Integer] {
      def toWire(x: java.lang.Integer, out: DataOutput) { out.writeInt(x) }
      def fromWire(in: DataInput): java.lang.Integer = in.readInt()
      def show(x: java.lang.Integer) = x.toString
  }

  implicit def BooleanFmt = new WireFormat[Boolean] {
    def toWire(x: Boolean, out: DataOutput) { out.writeBoolean(x) }
    def fromWire(in: DataInput): Boolean = in.readBoolean()
    def show(x: Boolean) = x.toString
  }

  implicit def LongFmt = new WireFormat[Long] {
    def toWire(x: Long, out: DataOutput) { out.writeLong(x) }
    def fromWire(in: DataInput): Long = in.readLong()
    def show(x: Long) = x.toString
  }

  implicit def DoubleFmt = new WireFormat[Double] {
    def toWire(x: Double, out: DataOutput) { out.writeDouble(x) }
    def fromWire(in: DataInput): Double = { in.readDouble() }
    def show(x: Double) = x.toString
  }

  implicit def CharFmt = new WireFormat[Char] {
    def toWire(x: Char, out: DataOutput) { out.writeChar(x) }
    def fromWire(in: DataInput): Char = in.readChar()
    def show(x: Char) = x.toString
  }

  implicit def ByteFmt = new WireFormat[Byte] {
    def toWire(x: Byte, out: DataOutput) { out.writeByte(x) }
    def fromWire(in: DataInput): Byte = in.readByte()
    def show(x: Byte) = x.toString
  }

  implicit def StringFmt = new WireFormat[String] {
    def toWire(x: String, out: DataOutput) {
      require(x != null, "Error, trying to serialize a null String. Consider using an empty string or Option[String]")
      val b = x.getBytes("utf-8")
      out.writeInt(b.length)
      out.write(b)
    }
    def fromWire(in: DataInput): String = {
      val l = in.readInt()
      val b = new Array[Byte](l)
      in.readFully(b, 0, l)
      new String(b, "utf-8")
    }
    def show(x: String) = x.toString
  }

  /*
   * Useful Scala types.
   */
  implicit def Tuple2Fmt[T1, T2](implicit wt1: WireFormat[T1], wt2: WireFormat[T2]) =
    new WireFormat[(T1, T2)] {
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

  implicit def Tuple3Fmt[T1, T2, T3]
      (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3]) =
    new WireFormat[(T1, T2, T3)] {
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

  implicit def Tuple4Fmt[T1, T2, T3, T4]
      (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4]) =
    new WireFormat[Tuple4[T1, T2, T3, T4]] {
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

  implicit def Tuple5Fmt[T1, T2, T3, T4, T5]
      (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4],
                wt5: WireFormat[T5]) =
    new WireFormat[(T1, T2, T3, T4, T5)] {
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

    implicit def Tuple6Fmt[T1, T2, T3, T4, T5, T6]
      (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4],
                wt5: WireFormat[T5], wt6: WireFormat[T6]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6)] {
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

  implicit def Tuple7Fmt[T1, T2, T3, T4, T5, T6, T7]
      (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4],
                wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7)] {
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

  implicit def Tuple8Fmt[T1, T2, T3, T4, T5, T6, T7, T8]
      (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4],
                wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8)] {
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
  implicit def IterableFmt[T](implicit wt: WireFormat[T]) = new WireFormat[Iterable[T]] {
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
  implicit def OptionFmt[T](implicit wt: WireFormat[T]) = new WireFormat[Option[T]] {
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
  implicit def EitherFmt[T1, T2](implicit wt1: WireFormat[T1], wt2: WireFormat[T2]) = new WireFormat[Either[T1, T2]] {
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

  /*
   * Java's Date
   */

  implicit def DateFmt = new WireFormat[java.util.Date] {
    def toWire(x: java.util.Date, out: DataOutput) = out.writeLong(x.getTime)
    def fromWire(in: DataInput): java.util.Date = new java.util.Date(in.readLong())
    def show(x: java.util.Date) = x.toString
  }
}

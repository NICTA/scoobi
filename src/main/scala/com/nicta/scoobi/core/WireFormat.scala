package com.nicta.scoobi
package core

import annotation.implicitNotFound
import java.io._
import impl.slow
import org.apache.hadoop.io.Writable
import collection.generic.CanBuildFrom
import collection.mutable.{ArrayBuilder, Builder}

/**Type-class for sending types across the Hadoop wire. */
@implicitNotFound(msg = "Cannot find WireFormat type class for ${A}")
trait WireFormat[A] {
  def toWire(x: A, out: DataOutput)

  def fromWire(in: DataInput): A
}


object WireFormat extends WireFormatImplicits

/** Implicit definitions of WireFormat instances for common types. */
trait WireFormatImplicits {

  def mkObjectWireFormat[T](x: T) = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {}
    override def fromWire(in: DataInput): T = x
  }

  def mkCaseWireFormat[T](apply: () => T, unapply: T => Boolean): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {}
    override def fromWire(in: DataInput): T = apply()
  }

  def mkCaseWireFormat[T, A1: WireFormat](apply: (A1) => T, unapply: T => Option[(A1)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      implicitly[WireFormat[A1]].toWire(unapply(obj).get, out)
    }
    override def fromWire(in: DataInput): T = apply(implicitly[WireFormat[A1]].fromWire(in))
  }

  /* ------------- AUTO-GENERATED CODE BELOW ------------- */
  /* ------------- DON'T MODIFY IT BY HAND!!!! ------------- */
  /* To generate the code below, use the file GenWireFormat.scala.interp in
     this directory.  Run 'scala' from this directory, then do the following:

     :load GenWireFormat.scala.interp
     GenWireFormat.gen_files()

     This generates two files called 'sec1.ins' and 'sec2.ins'.  Load
     'sec1.ins' here, between the BEGIN/END comments, deleting any existing
     code.  Load 'sec2.ins' down below, in the second auto-generated section.
     Then delete those two files.
     */

  /* ------------- BEGIN AUTO-GENERATED SECTION 1 ------------- */
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
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product9[A1, A2, A3, A4, A5, A6, A7, A8, A9] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat, A13: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
      implicitly[WireFormat[A13]].toWire(v._13, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      val a13: A13 = implicitly[WireFormat[A13]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat, A13: WireFormat, A14: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
      implicitly[WireFormat[A13]].toWire(v._13, out)
      implicitly[WireFormat[A14]].toWire(v._14, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      val a13: A13 = implicitly[WireFormat[A13]].fromWire(in)
      val a14: A14 = implicitly[WireFormat[A14]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat, A13: WireFormat, A14: WireFormat, A15: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
      implicitly[WireFormat[A13]].toWire(v._13, out)
      implicitly[WireFormat[A14]].toWire(v._14, out)
      implicitly[WireFormat[A15]].toWire(v._15, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      val a13: A13 = implicitly[WireFormat[A13]].fromWire(in)
      val a14: A14 = implicitly[WireFormat[A14]].fromWire(in)
      val a15: A15 = implicitly[WireFormat[A15]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat, A13: WireFormat, A14: WireFormat, A15: WireFormat, A16: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
      implicitly[WireFormat[A13]].toWire(v._13, out)
      implicitly[WireFormat[A14]].toWire(v._14, out)
      implicitly[WireFormat[A15]].toWire(v._15, out)
      implicitly[WireFormat[A16]].toWire(v._16, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      val a13: A13 = implicitly[WireFormat[A13]].fromWire(in)
      val a14: A14 = implicitly[WireFormat[A14]].fromWire(in)
      val a15: A15 = implicitly[WireFormat[A15]].fromWire(in)
      val a16: A16 = implicitly[WireFormat[A16]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat, A13: WireFormat, A14: WireFormat, A15: WireFormat, A16: WireFormat, A17: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product17[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
      implicitly[WireFormat[A13]].toWire(v._13, out)
      implicitly[WireFormat[A14]].toWire(v._14, out)
      implicitly[WireFormat[A15]].toWire(v._15, out)
      implicitly[WireFormat[A16]].toWire(v._16, out)
      implicitly[WireFormat[A17]].toWire(v._17, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      val a13: A13 = implicitly[WireFormat[A13]].fromWire(in)
      val a14: A14 = implicitly[WireFormat[A14]].fromWire(in)
      val a15: A15 = implicitly[WireFormat[A15]].fromWire(in)
      val a16: A16 = implicitly[WireFormat[A16]].fromWire(in)
      val a17: A17 = implicitly[WireFormat[A17]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat, A13: WireFormat, A14: WireFormat, A15: WireFormat, A16: WireFormat, A17: WireFormat, A18: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product18[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
      implicitly[WireFormat[A13]].toWire(v._13, out)
      implicitly[WireFormat[A14]].toWire(v._14, out)
      implicitly[WireFormat[A15]].toWire(v._15, out)
      implicitly[WireFormat[A16]].toWire(v._16, out)
      implicitly[WireFormat[A17]].toWire(v._17, out)
      implicitly[WireFormat[A18]].toWire(v._18, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      val a13: A13 = implicitly[WireFormat[A13]].fromWire(in)
      val a14: A14 = implicitly[WireFormat[A14]].fromWire(in)
      val a15: A15 = implicitly[WireFormat[A15]].fromWire(in)
      val a16: A16 = implicitly[WireFormat[A16]].fromWire(in)
      val a17: A17 = implicitly[WireFormat[A17]].fromWire(in)
      val a18: A18 = implicitly[WireFormat[A18]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat, A13: WireFormat, A14: WireFormat, A15: WireFormat, A16: WireFormat, A17: WireFormat, A18: WireFormat, A19: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product19[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
      implicitly[WireFormat[A13]].toWire(v._13, out)
      implicitly[WireFormat[A14]].toWire(v._14, out)
      implicitly[WireFormat[A15]].toWire(v._15, out)
      implicitly[WireFormat[A16]].toWire(v._16, out)
      implicitly[WireFormat[A17]].toWire(v._17, out)
      implicitly[WireFormat[A18]].toWire(v._18, out)
      implicitly[WireFormat[A19]].toWire(v._19, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      val a13: A13 = implicitly[WireFormat[A13]].fromWire(in)
      val a14: A14 = implicitly[WireFormat[A14]].fromWire(in)
      val a15: A15 = implicitly[WireFormat[A15]].fromWire(in)
      val a16: A16 = implicitly[WireFormat[A16]].fromWire(in)
      val a17: A17 = implicitly[WireFormat[A17]].fromWire(in)
      val a18: A18 = implicitly[WireFormat[A18]].fromWire(in)
      val a19: A19 = implicitly[WireFormat[A19]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat, A13: WireFormat, A14: WireFormat, A15: WireFormat, A16: WireFormat, A17: WireFormat, A18: WireFormat, A19: WireFormat, A20: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product20[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
      implicitly[WireFormat[A13]].toWire(v._13, out)
      implicitly[WireFormat[A14]].toWire(v._14, out)
      implicitly[WireFormat[A15]].toWire(v._15, out)
      implicitly[WireFormat[A16]].toWire(v._16, out)
      implicitly[WireFormat[A17]].toWire(v._17, out)
      implicitly[WireFormat[A18]].toWire(v._18, out)
      implicitly[WireFormat[A19]].toWire(v._19, out)
      implicitly[WireFormat[A20]].toWire(v._20, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      val a13: A13 = implicitly[WireFormat[A13]].fromWire(in)
      val a14: A14 = implicitly[WireFormat[A14]].fromWire(in)
      val a15: A15 = implicitly[WireFormat[A15]].fromWire(in)
      val a16: A16 = implicitly[WireFormat[A16]].fromWire(in)
      val a17: A17 = implicitly[WireFormat[A17]].fromWire(in)
      val a18: A18 = implicitly[WireFormat[A18]].fromWire(in)
      val a19: A19 = implicitly[WireFormat[A19]].fromWire(in)
      val a20: A20 = implicitly[WireFormat[A20]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat, A13: WireFormat, A14: WireFormat, A15: WireFormat, A16: WireFormat, A17: WireFormat, A18: WireFormat, A19: WireFormat, A20: WireFormat, A21: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product21[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
      implicitly[WireFormat[A13]].toWire(v._13, out)
      implicitly[WireFormat[A14]].toWire(v._14, out)
      implicitly[WireFormat[A15]].toWire(v._15, out)
      implicitly[WireFormat[A16]].toWire(v._16, out)
      implicitly[WireFormat[A17]].toWire(v._17, out)
      implicitly[WireFormat[A18]].toWire(v._18, out)
      implicitly[WireFormat[A19]].toWire(v._19, out)
      implicitly[WireFormat[A20]].toWire(v._20, out)
      implicitly[WireFormat[A21]].toWire(v._21, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      val a13: A13 = implicitly[WireFormat[A13]].fromWire(in)
      val a14: A14 = implicitly[WireFormat[A14]].fromWire(in)
      val a15: A15 = implicitly[WireFormat[A15]].fromWire(in)
      val a16: A16 = implicitly[WireFormat[A16]].fromWire(in)
      val a17: A17 = implicitly[WireFormat[A17]].fromWire(in)
      val a18: A18 = implicitly[WireFormat[A18]].fromWire(in)
      val a19: A19 = implicitly[WireFormat[A19]].fromWire(in)
      val a20: A20 = implicitly[WireFormat[A20]].fromWire(in)
      val a21: A21 = implicitly[WireFormat[A21]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21)
    }
  }

  def mkCaseWireFormat[T, A1: WireFormat, A2: WireFormat, A3: WireFormat, A4: WireFormat, A5: WireFormat, A6: WireFormat, A7: WireFormat, A8: WireFormat, A9: WireFormat, A10: WireFormat, A11: WireFormat, A12: WireFormat, A13: WireFormat, A14: WireFormat, A15: WireFormat, A16: WireFormat, A17: WireFormat, A18: WireFormat, A19: WireFormat, A20: WireFormat, A21: WireFormat, A22: WireFormat](apply: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22)]): WireFormat[T] = new WireFormat[T] {
    override def toWire(obj: T, out: DataOutput) {
      val v: Product22[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22] = unapply(obj).get
      implicitly[WireFormat[A1]].toWire(v._1, out)
      implicitly[WireFormat[A2]].toWire(v._2, out)
      implicitly[WireFormat[A3]].toWire(v._3, out)
      implicitly[WireFormat[A4]].toWire(v._4, out)
      implicitly[WireFormat[A5]].toWire(v._5, out)
      implicitly[WireFormat[A6]].toWire(v._6, out)
      implicitly[WireFormat[A7]].toWire(v._7, out)
      implicitly[WireFormat[A8]].toWire(v._8, out)
      implicitly[WireFormat[A9]].toWire(v._9, out)
      implicitly[WireFormat[A10]].toWire(v._10, out)
      implicitly[WireFormat[A11]].toWire(v._11, out)
      implicitly[WireFormat[A12]].toWire(v._12, out)
      implicitly[WireFormat[A13]].toWire(v._13, out)
      implicitly[WireFormat[A14]].toWire(v._14, out)
      implicitly[WireFormat[A15]].toWire(v._15, out)
      implicitly[WireFormat[A16]].toWire(v._16, out)
      implicitly[WireFormat[A17]].toWire(v._17, out)
      implicitly[WireFormat[A18]].toWire(v._18, out)
      implicitly[WireFormat[A19]].toWire(v._19, out)
      implicitly[WireFormat[A20]].toWire(v._20, out)
      implicitly[WireFormat[A21]].toWire(v._21, out)
      implicitly[WireFormat[A22]].toWire(v._22, out)
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
      val a9: A9 = implicitly[WireFormat[A9]].fromWire(in)
      val a10: A10 = implicitly[WireFormat[A10]].fromWire(in)
      val a11: A11 = implicitly[WireFormat[A11]].fromWire(in)
      val a12: A12 = implicitly[WireFormat[A12]].fromWire(in)
      val a13: A13 = implicitly[WireFormat[A13]].fromWire(in)
      val a14: A14 = implicitly[WireFormat[A14]].fromWire(in)
      val a15: A15 = implicitly[WireFormat[A15]].fromWire(in)
      val a16: A16 = implicitly[WireFormat[A16]].fromWire(in)
      val a17: A17 = implicitly[WireFormat[A17]].fromWire(in)
      val a18: A18 = implicitly[WireFormat[A18]].fromWire(in)
      val a19: A19 = implicitly[WireFormat[A19]].fromWire(in)
      val a20: A20 = implicitly[WireFormat[A20]].fromWire(in)
      val a21: A21 = implicitly[WireFormat[A21]].fromWire(in)
      val a22: A22 = implicitly[WireFormat[A22]].fromWire(in)
      apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22)
    }
  }
  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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

    override def fromWire(in: DataInput): TT =
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
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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

    override def fromWire(in: DataInput): TT =
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
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat, M <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else if (clazz == implicitly[Manifest[M]].erasure) {
        out.writeInt('M')
        implicitly[WireFormat[M]].toWire(obj.asInstanceOf[M], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case 'M' => implicitly[WireFormat[M]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat, M <: TT : Manifest : WireFormat, N <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else if (clazz == implicitly[Manifest[M]].erasure) {
        out.writeInt('M')
        implicitly[WireFormat[M]].toWire(obj.asInstanceOf[M], out)
      } else if (clazz == implicitly[Manifest[N]].erasure) {
        out.writeInt('N')
        implicitly[WireFormat[N]].toWire(obj.asInstanceOf[N], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case 'M' => implicitly[WireFormat[M]].fromWire(in)
        case 'N' => implicitly[WireFormat[N]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat, M <: TT : Manifest : WireFormat, N <: TT : Manifest : WireFormat, O <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else if (clazz == implicitly[Manifest[M]].erasure) {
        out.writeInt('M')
        implicitly[WireFormat[M]].toWire(obj.asInstanceOf[M], out)
      } else if (clazz == implicitly[Manifest[N]].erasure) {
        out.writeInt('N')
        implicitly[WireFormat[N]].toWire(obj.asInstanceOf[N], out)
      } else if (clazz == implicitly[Manifest[O]].erasure) {
        out.writeInt('O')
        implicitly[WireFormat[O]].toWire(obj.asInstanceOf[O], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case 'M' => implicitly[WireFormat[M]].fromWire(in)
        case 'N' => implicitly[WireFormat[N]].fromWire(in)
        case 'O' => implicitly[WireFormat[O]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat, M <: TT : Manifest : WireFormat, N <: TT : Manifest : WireFormat, O <: TT : Manifest : WireFormat, P <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else if (clazz == implicitly[Manifest[M]].erasure) {
        out.writeInt('M')
        implicitly[WireFormat[M]].toWire(obj.asInstanceOf[M], out)
      } else if (clazz == implicitly[Manifest[N]].erasure) {
        out.writeInt('N')
        implicitly[WireFormat[N]].toWire(obj.asInstanceOf[N], out)
      } else if (clazz == implicitly[Manifest[O]].erasure) {
        out.writeInt('O')
        implicitly[WireFormat[O]].toWire(obj.asInstanceOf[O], out)
      } else if (clazz == implicitly[Manifest[P]].erasure) {
        out.writeInt('P')
        implicitly[WireFormat[P]].toWire(obj.asInstanceOf[P], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case 'M' => implicitly[WireFormat[M]].fromWire(in)
        case 'N' => implicitly[WireFormat[N]].fromWire(in)
        case 'O' => implicitly[WireFormat[O]].fromWire(in)
        case 'P' => implicitly[WireFormat[P]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat, M <: TT : Manifest : WireFormat, N <: TT : Manifest : WireFormat, O <: TT : Manifest : WireFormat, P <: TT : Manifest : WireFormat, Q <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else if (clazz == implicitly[Manifest[M]].erasure) {
        out.writeInt('M')
        implicitly[WireFormat[M]].toWire(obj.asInstanceOf[M], out)
      } else if (clazz == implicitly[Manifest[N]].erasure) {
        out.writeInt('N')
        implicitly[WireFormat[N]].toWire(obj.asInstanceOf[N], out)
      } else if (clazz == implicitly[Manifest[O]].erasure) {
        out.writeInt('O')
        implicitly[WireFormat[O]].toWire(obj.asInstanceOf[O], out)
      } else if (clazz == implicitly[Manifest[P]].erasure) {
        out.writeInt('P')
        implicitly[WireFormat[P]].toWire(obj.asInstanceOf[P], out)
      } else if (clazz == implicitly[Manifest[Q]].erasure) {
        out.writeInt('Q')
        implicitly[WireFormat[Q]].toWire(obj.asInstanceOf[Q], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case 'M' => implicitly[WireFormat[M]].fromWire(in)
        case 'N' => implicitly[WireFormat[N]].fromWire(in)
        case 'O' => implicitly[WireFormat[O]].fromWire(in)
        case 'P' => implicitly[WireFormat[P]].fromWire(in)
        case 'Q' => implicitly[WireFormat[Q]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat, M <: TT : Manifest : WireFormat, N <: TT : Manifest : WireFormat, O <: TT : Manifest : WireFormat, P <: TT : Manifest : WireFormat, Q <: TT : Manifest : WireFormat, R <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else if (clazz == implicitly[Manifest[M]].erasure) {
        out.writeInt('M')
        implicitly[WireFormat[M]].toWire(obj.asInstanceOf[M], out)
      } else if (clazz == implicitly[Manifest[N]].erasure) {
        out.writeInt('N')
        implicitly[WireFormat[N]].toWire(obj.asInstanceOf[N], out)
      } else if (clazz == implicitly[Manifest[O]].erasure) {
        out.writeInt('O')
        implicitly[WireFormat[O]].toWire(obj.asInstanceOf[O], out)
      } else if (clazz == implicitly[Manifest[P]].erasure) {
        out.writeInt('P')
        implicitly[WireFormat[P]].toWire(obj.asInstanceOf[P], out)
      } else if (clazz == implicitly[Manifest[Q]].erasure) {
        out.writeInt('Q')
        implicitly[WireFormat[Q]].toWire(obj.asInstanceOf[Q], out)
      } else if (clazz == implicitly[Manifest[R]].erasure) {
        out.writeInt('R')
        implicitly[WireFormat[R]].toWire(obj.asInstanceOf[R], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case 'M' => implicitly[WireFormat[M]].fromWire(in)
        case 'N' => implicitly[WireFormat[N]].fromWire(in)
        case 'O' => implicitly[WireFormat[O]].fromWire(in)
        case 'P' => implicitly[WireFormat[P]].fromWire(in)
        case 'Q' => implicitly[WireFormat[Q]].fromWire(in)
        case 'R' => implicitly[WireFormat[R]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat, M <: TT : Manifest : WireFormat, N <: TT : Manifest : WireFormat, O <: TT : Manifest : WireFormat, P <: TT : Manifest : WireFormat, Q <: TT : Manifest : WireFormat, R <: TT : Manifest : WireFormat, S <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else if (clazz == implicitly[Manifest[M]].erasure) {
        out.writeInt('M')
        implicitly[WireFormat[M]].toWire(obj.asInstanceOf[M], out)
      } else if (clazz == implicitly[Manifest[N]].erasure) {
        out.writeInt('N')
        implicitly[WireFormat[N]].toWire(obj.asInstanceOf[N], out)
      } else if (clazz == implicitly[Manifest[O]].erasure) {
        out.writeInt('O')
        implicitly[WireFormat[O]].toWire(obj.asInstanceOf[O], out)
      } else if (clazz == implicitly[Manifest[P]].erasure) {
        out.writeInt('P')
        implicitly[WireFormat[P]].toWire(obj.asInstanceOf[P], out)
      } else if (clazz == implicitly[Manifest[Q]].erasure) {
        out.writeInt('Q')
        implicitly[WireFormat[Q]].toWire(obj.asInstanceOf[Q], out)
      } else if (clazz == implicitly[Manifest[R]].erasure) {
        out.writeInt('R')
        implicitly[WireFormat[R]].toWire(obj.asInstanceOf[R], out)
      } else if (clazz == implicitly[Manifest[S]].erasure) {
        out.writeInt('S')
        implicitly[WireFormat[S]].toWire(obj.asInstanceOf[S], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case 'M' => implicitly[WireFormat[M]].fromWire(in)
        case 'N' => implicitly[WireFormat[N]].fromWire(in)
        case 'O' => implicitly[WireFormat[O]].fromWire(in)
        case 'P' => implicitly[WireFormat[P]].fromWire(in)
        case 'Q' => implicitly[WireFormat[Q]].fromWire(in)
        case 'R' => implicitly[WireFormat[R]].fromWire(in)
        case 'S' => implicitly[WireFormat[S]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat, M <: TT : Manifest : WireFormat, N <: TT : Manifest : WireFormat, O <: TT : Manifest : WireFormat, P <: TT : Manifest : WireFormat, Q <: TT : Manifest : WireFormat, R <: TT : Manifest : WireFormat, S <: TT : Manifest : WireFormat, T <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else if (clazz == implicitly[Manifest[M]].erasure) {
        out.writeInt('M')
        implicitly[WireFormat[M]].toWire(obj.asInstanceOf[M], out)
      } else if (clazz == implicitly[Manifest[N]].erasure) {
        out.writeInt('N')
        implicitly[WireFormat[N]].toWire(obj.asInstanceOf[N], out)
      } else if (clazz == implicitly[Manifest[O]].erasure) {
        out.writeInt('O')
        implicitly[WireFormat[O]].toWire(obj.asInstanceOf[O], out)
      } else if (clazz == implicitly[Manifest[P]].erasure) {
        out.writeInt('P')
        implicitly[WireFormat[P]].toWire(obj.asInstanceOf[P], out)
      } else if (clazz == implicitly[Manifest[Q]].erasure) {
        out.writeInt('Q')
        implicitly[WireFormat[Q]].toWire(obj.asInstanceOf[Q], out)
      } else if (clazz == implicitly[Manifest[R]].erasure) {
        out.writeInt('R')
        implicitly[WireFormat[R]].toWire(obj.asInstanceOf[R], out)
      } else if (clazz == implicitly[Manifest[S]].erasure) {
        out.writeInt('S')
        implicitly[WireFormat[S]].toWire(obj.asInstanceOf[S], out)
      } else if (clazz == implicitly[Manifest[T]].erasure) {
        out.writeInt('T')
        implicitly[WireFormat[T]].toWire(obj.asInstanceOf[T], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case 'M' => implicitly[WireFormat[M]].fromWire(in)
        case 'N' => implicitly[WireFormat[N]].fromWire(in)
        case 'O' => implicitly[WireFormat[O]].fromWire(in)
        case 'P' => implicitly[WireFormat[P]].fromWire(in)
        case 'Q' => implicitly[WireFormat[Q]].fromWire(in)
        case 'R' => implicitly[WireFormat[R]].fromWire(in)
        case 'S' => implicitly[WireFormat[S]].fromWire(in)
        case 'T' => implicitly[WireFormat[T]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat, M <: TT : Manifest : WireFormat, N <: TT : Manifest : WireFormat, O <: TT : Manifest : WireFormat, P <: TT : Manifest : WireFormat, Q <: TT : Manifest : WireFormat, R <: TT : Manifest : WireFormat, S <: TT : Manifest : WireFormat, T <: TT : Manifest : WireFormat, U <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else if (clazz == implicitly[Manifest[M]].erasure) {
        out.writeInt('M')
        implicitly[WireFormat[M]].toWire(obj.asInstanceOf[M], out)
      } else if (clazz == implicitly[Manifest[N]].erasure) {
        out.writeInt('N')
        implicitly[WireFormat[N]].toWire(obj.asInstanceOf[N], out)
      } else if (clazz == implicitly[Manifest[O]].erasure) {
        out.writeInt('O')
        implicitly[WireFormat[O]].toWire(obj.asInstanceOf[O], out)
      } else if (clazz == implicitly[Manifest[P]].erasure) {
        out.writeInt('P')
        implicitly[WireFormat[P]].toWire(obj.asInstanceOf[P], out)
      } else if (clazz == implicitly[Manifest[Q]].erasure) {
        out.writeInt('Q')
        implicitly[WireFormat[Q]].toWire(obj.asInstanceOf[Q], out)
      } else if (clazz == implicitly[Manifest[R]].erasure) {
        out.writeInt('R')
        implicitly[WireFormat[R]].toWire(obj.asInstanceOf[R], out)
      } else if (clazz == implicitly[Manifest[S]].erasure) {
        out.writeInt('S')
        implicitly[WireFormat[S]].toWire(obj.asInstanceOf[S], out)
      } else if (clazz == implicitly[Manifest[T]].erasure) {
        out.writeInt('T')
        implicitly[WireFormat[T]].toWire(obj.asInstanceOf[T], out)
      } else if (clazz == implicitly[Manifest[U]].erasure) {
        out.writeInt('U')
        implicitly[WireFormat[U]].toWire(obj.asInstanceOf[U], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case 'M' => implicitly[WireFormat[M]].fromWire(in)
        case 'N' => implicitly[WireFormat[N]].fromWire(in)
        case 'O' => implicitly[WireFormat[O]].fromWire(in)
        case 'P' => implicitly[WireFormat[P]].fromWire(in)
        case 'Q' => implicitly[WireFormat[Q]].fromWire(in)
        case 'R' => implicitly[WireFormat[R]].fromWire(in)
        case 'S' => implicitly[WireFormat[S]].fromWire(in)
        case 'T' => implicitly[WireFormat[T]].fromWire(in)
        case 'U' => implicitly[WireFormat[U]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }

  def mkAbstractWireFormat[TT, A <: TT : Manifest : WireFormat, B <: TT : Manifest : WireFormat, C <: TT : Manifest : WireFormat, D <: TT : Manifest : WireFormat, E <: TT : Manifest : WireFormat, F <: TT : Manifest : WireFormat, G <: TT : Manifest : WireFormat, H <: TT : Manifest : WireFormat, I <: TT : Manifest : WireFormat, J <: TT : Manifest : WireFormat, K <: TT : Manifest : WireFormat, L <: TT : Manifest : WireFormat, M <: TT : Manifest : WireFormat, N <: TT : Manifest : WireFormat, O <: TT : Manifest : WireFormat, P <: TT : Manifest : WireFormat, Q <: TT : Manifest : WireFormat, R <: TT : Manifest : WireFormat, S <: TT : Manifest : WireFormat, T <: TT : Manifest : WireFormat, U <: TT : Manifest : WireFormat, V <: TT : Manifest : WireFormat]() = new WireFormat[TT] {

    override def toWire(obj: TT, out: DataOutput) {
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
      } else if (clazz == implicitly[Manifest[I]].erasure) {
        out.writeInt('I')
        implicitly[WireFormat[I]].toWire(obj.asInstanceOf[I], out)
      } else if (clazz == implicitly[Manifest[J]].erasure) {
        out.writeInt('J')
        implicitly[WireFormat[J]].toWire(obj.asInstanceOf[J], out)
      } else if (clazz == implicitly[Manifest[K]].erasure) {
        out.writeInt('K')
        implicitly[WireFormat[K]].toWire(obj.asInstanceOf[K], out)
      } else if (clazz == implicitly[Manifest[L]].erasure) {
        out.writeInt('L')
        implicitly[WireFormat[L]].toWire(obj.asInstanceOf[L], out)
      } else if (clazz == implicitly[Manifest[M]].erasure) {
        out.writeInt('M')
        implicitly[WireFormat[M]].toWire(obj.asInstanceOf[M], out)
      } else if (clazz == implicitly[Manifest[N]].erasure) {
        out.writeInt('N')
        implicitly[WireFormat[N]].toWire(obj.asInstanceOf[N], out)
      } else if (clazz == implicitly[Manifest[O]].erasure) {
        out.writeInt('O')
        implicitly[WireFormat[O]].toWire(obj.asInstanceOf[O], out)
      } else if (clazz == implicitly[Manifest[P]].erasure) {
        out.writeInt('P')
        implicitly[WireFormat[P]].toWire(obj.asInstanceOf[P], out)
      } else if (clazz == implicitly[Manifest[Q]].erasure) {
        out.writeInt('Q')
        implicitly[WireFormat[Q]].toWire(obj.asInstanceOf[Q], out)
      } else if (clazz == implicitly[Manifest[R]].erasure) {
        out.writeInt('R')
        implicitly[WireFormat[R]].toWire(obj.asInstanceOf[R], out)
      } else if (clazz == implicitly[Manifest[S]].erasure) {
        out.writeInt('S')
        implicitly[WireFormat[S]].toWire(obj.asInstanceOf[S], out)
      } else if (clazz == implicitly[Manifest[T]].erasure) {
        out.writeInt('T')
        implicitly[WireFormat[T]].toWire(obj.asInstanceOf[T], out)
      } else if (clazz == implicitly[Manifest[U]].erasure) {
        out.writeInt('U')
        implicitly[WireFormat[U]].toWire(obj.asInstanceOf[U], out)
      } else if (clazz == implicitly[Manifest[V]].erasure) {
        out.writeInt('V')
        implicitly[WireFormat[V]].toWire(obj.asInstanceOf[V], out)
      } else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }

    override def fromWire(in: DataInput): TT =
      in.readInt() match {
        case 'A' => implicitly[WireFormat[A]].fromWire(in)
        case 'B' => implicitly[WireFormat[B]].fromWire(in)
        case 'C' => implicitly[WireFormat[C]].fromWire(in)
        case 'D' => implicitly[WireFormat[D]].fromWire(in)
        case 'E' => implicitly[WireFormat[E]].fromWire(in)
        case 'F' => implicitly[WireFormat[F]].fromWire(in)
        case 'G' => implicitly[WireFormat[G]].fromWire(in)
        case 'H' => implicitly[WireFormat[H]].fromWire(in)
        case 'I' => implicitly[WireFormat[I]].fromWire(in)
        case 'J' => implicitly[WireFormat[J]].fromWire(in)
        case 'K' => implicitly[WireFormat[K]].fromWire(in)
        case 'L' => implicitly[WireFormat[L]].fromWire(in)
        case 'M' => implicitly[WireFormat[M]].fromWire(in)
        case 'N' => implicitly[WireFormat[N]].fromWire(in)
        case 'O' => implicitly[WireFormat[O]].fromWire(in)
        case 'P' => implicitly[WireFormat[P]].fromWire(in)
        case 'Q' => implicitly[WireFormat[Q]].fromWire(in)
        case 'R' => implicitly[WireFormat[R]].fromWire(in)
        case 'S' => implicitly[WireFormat[S]].fromWire(in)
        case 'T' => implicitly[WireFormat[T]].fromWire(in)
        case 'U' => implicitly[WireFormat[U]].fromWire(in)
        case 'V' => implicitly[WireFormat[V]].fromWire(in)
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
  }
  /* ------------- END AUTO-GENERATED SECTION 1 ------------- */

  /**
   * Catch-all
   */
  @slow("You are using inefficient serialization, try creating an explicit WireFormat instance instead", "since 0.1")
  implicit def AnythingFmt[T <: Serializable] = new WireFormat[T] {
    def toWire(x: T, out: DataOutput) {
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
  }

  /**
   * Hadoop Writable types.
   */
  implicit def WritableFmt[T <: Writable : Manifest] = new WireFormat[T] {
    def toWire(x: T, out: DataOutput) { x.write(out) }
    def fromWire(in: DataInput): T = {
      val x: T = implicitly[Manifest[T]].erasure.newInstance.asInstanceOf[T]
      x.readFields(in)
      x
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

  /* ------------- AUTO-GENERATED CODE BELOW ------------- */
  /* ------------- DON'T MODIFY IT BY HAND!!!! ------------- */

  /* See instructions above for how to regenerate the code below. */
  /* ------------- BEGIN AUTO-GENERATED SECTION 2 ------------- */
  implicit def Tuple2Fmt[T1, T2]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2]) =
    new WireFormat[(T1, T2)] {
      def toWire(x: (T1, T2), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
      }
      def fromWire(in: DataInput): (T1, T2) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        (a, b)
      }
    }

  implicit def Tuple3Fmt[T1, T2, T3]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3]) =
    new WireFormat[(T1, T2, T3)] {
      def toWire(x: (T1, T2, T3), out: DataOutput) {
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
    }

  implicit def Tuple4Fmt[T1, T2, T3, T4]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4]) =
    new WireFormat[(T1, T2, T3, T4)] {
      def toWire(x: (T1, T2, T3, T4), out: DataOutput) {
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
    }

  implicit def Tuple5Fmt[T1, T2, T3, T4, T5]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5]) =
    new WireFormat[(T1, T2, T3, T4, T5)] {
      def toWire(x: (T1, T2, T3, T4, T5), out: DataOutput) {
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
    }

  implicit def Tuple6Fmt[T1, T2, T3, T4, T5, T6]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6), out: DataOutput) {
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
    }

  implicit def Tuple7Fmt[T1, T2, T3, T4, T5, T6, T7]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7), out: DataOutput) {
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
    }

  implicit def Tuple8Fmt[T1, T2, T3, T4, T5, T6, T7, T8]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8), out: DataOutput) {
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
    }

  implicit def Tuple9Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        (a, b, c, d, e, f, g, h, i)
      }
    }

  implicit def Tuple10Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j)
      }
    }

  implicit def Tuple11Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k)
      }
    }

  implicit def Tuple12Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l)
      }
    }

  implicit def Tuple13Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12], wt13: WireFormat[T13]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
        wt13.toWire(x._13, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        val m = wt13.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l, m)
      }
    }

  implicit def Tuple14Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12], wt13: WireFormat[T13], wt14: WireFormat[T14]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
        wt13.toWire(x._13, out)
        wt14.toWire(x._14, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        val m = wt13.fromWire(in)
        val n = wt14.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n)
      }
    }

  implicit def Tuple15Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12], wt13: WireFormat[T13], wt14: WireFormat[T14], wt15: WireFormat[T15]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
        wt13.toWire(x._13, out)
        wt14.toWire(x._14, out)
        wt15.toWire(x._15, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        val m = wt13.fromWire(in)
        val n = wt14.fromWire(in)
        val o = wt15.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
      }
    }

  implicit def Tuple16Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12], wt13: WireFormat[T13], wt14: WireFormat[T14], wt15: WireFormat[T15], wt16: WireFormat[T16]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
        wt13.toWire(x._13, out)
        wt14.toWire(x._14, out)
        wt15.toWire(x._15, out)
        wt16.toWire(x._16, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        val m = wt13.fromWire(in)
        val n = wt14.fromWire(in)
        val o = wt15.fromWire(in)
        val p = wt16.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
      }
    }

  implicit def Tuple17Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12], wt13: WireFormat[T13], wt14: WireFormat[T14], wt15: WireFormat[T15], wt16: WireFormat[T16], wt17: WireFormat[T17]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
        wt13.toWire(x._13, out)
        wt14.toWire(x._14, out)
        wt15.toWire(x._15, out)
        wt16.toWire(x._16, out)
        wt17.toWire(x._17, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        val m = wt13.fromWire(in)
        val n = wt14.fromWire(in)
        val o = wt15.fromWire(in)
        val p = wt16.fromWire(in)
        val q = wt17.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
      }
    }

  implicit def Tuple18Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12], wt13: WireFormat[T13], wt14: WireFormat[T14], wt15: WireFormat[T15], wt16: WireFormat[T16], wt17: WireFormat[T17], wt18: WireFormat[T18]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
        wt13.toWire(x._13, out)
        wt14.toWire(x._14, out)
        wt15.toWire(x._15, out)
        wt16.toWire(x._16, out)
        wt17.toWire(x._17, out)
        wt18.toWire(x._18, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        val m = wt13.fromWire(in)
        val n = wt14.fromWire(in)
        val o = wt15.fromWire(in)
        val p = wt16.fromWire(in)
        val q = wt17.fromWire(in)
        val r = wt18.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
      }
    }

  implicit def Tuple19Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12], wt13: WireFormat[T13], wt14: WireFormat[T14], wt15: WireFormat[T15], wt16: WireFormat[T16], wt17: WireFormat[T17], wt18: WireFormat[T18], wt19: WireFormat[T19]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
        wt13.toWire(x._13, out)
        wt14.toWire(x._14, out)
        wt15.toWire(x._15, out)
        wt16.toWire(x._16, out)
        wt17.toWire(x._17, out)
        wt18.toWire(x._18, out)
        wt19.toWire(x._19, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        val m = wt13.fromWire(in)
        val n = wt14.fromWire(in)
        val o = wt15.fromWire(in)
        val p = wt16.fromWire(in)
        val q = wt17.fromWire(in)
        val r = wt18.fromWire(in)
        val s = wt19.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
      }
    }

  implicit def Tuple20Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12], wt13: WireFormat[T13], wt14: WireFormat[T14], wt15: WireFormat[T15], wt16: WireFormat[T16], wt17: WireFormat[T17], wt18: WireFormat[T18], wt19: WireFormat[T19], wt20: WireFormat[T20]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
        wt13.toWire(x._13, out)
        wt14.toWire(x._14, out)
        wt15.toWire(x._15, out)
        wt16.toWire(x._16, out)
        wt17.toWire(x._17, out)
        wt18.toWire(x._18, out)
        wt19.toWire(x._19, out)
        wt20.toWire(x._20, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        val m = wt13.fromWire(in)
        val n = wt14.fromWire(in)
        val o = wt15.fromWire(in)
        val p = wt16.fromWire(in)
        val q = wt17.fromWire(in)
        val r = wt18.fromWire(in)
        val s = wt19.fromWire(in)
        val t = wt20.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
      }
    }

  implicit def Tuple21Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12], wt13: WireFormat[T13], wt14: WireFormat[T14], wt15: WireFormat[T15], wt16: WireFormat[T16], wt17: WireFormat[T17], wt18: WireFormat[T18], wt19: WireFormat[T19], wt20: WireFormat[T20], wt21: WireFormat[T21]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
        wt13.toWire(x._13, out)
        wt14.toWire(x._14, out)
        wt15.toWire(x._15, out)
        wt16.toWire(x._16, out)
        wt17.toWire(x._17, out)
        wt18.toWire(x._18, out)
        wt19.toWire(x._19, out)
        wt20.toWire(x._20, out)
        wt21.toWire(x._21, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        val m = wt13.fromWire(in)
        val n = wt14.fromWire(in)
        val o = wt15.fromWire(in)
        val p = wt16.fromWire(in)
        val q = wt17.fromWire(in)
        val r = wt18.fromWire(in)
        val s = wt19.fromWire(in)
        val t = wt20.fromWire(in)
        val u = wt21.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
      }
    }

  implicit def Tuple22Fmt[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]
  (implicit wt1: WireFormat[T1], wt2: WireFormat[T2], wt3: WireFormat[T3], wt4: WireFormat[T4], wt5: WireFormat[T5], wt6: WireFormat[T6], wt7: WireFormat[T7], wt8: WireFormat[T8], wt9: WireFormat[T9], wt10: WireFormat[T10], wt11: WireFormat[T11], wt12: WireFormat[T12], wt13: WireFormat[T13], wt14: WireFormat[T14], wt15: WireFormat[T15], wt16: WireFormat[T16], wt17: WireFormat[T17], wt18: WireFormat[T18], wt19: WireFormat[T19], wt20: WireFormat[T20], wt21: WireFormat[T21], wt22: WireFormat[T22]) =
    new WireFormat[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22)] {
      def toWire(x: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22), out: DataOutput) {
        wt1.toWire(x._1, out)
        wt2.toWire(x._2, out)
        wt3.toWire(x._3, out)
        wt4.toWire(x._4, out)
        wt5.toWire(x._5, out)
        wt6.toWire(x._6, out)
        wt7.toWire(x._7, out)
        wt8.toWire(x._8, out)
        wt9.toWire(x._9, out)
        wt10.toWire(x._10, out)
        wt11.toWire(x._11, out)
        wt12.toWire(x._12, out)
        wt13.toWire(x._13, out)
        wt14.toWire(x._14, out)
        wt15.toWire(x._15, out)
        wt16.toWire(x._16, out)
        wt17.toWire(x._17, out)
        wt18.toWire(x._18, out)
        wt19.toWire(x._19, out)
        wt20.toWire(x._20, out)
        wt21.toWire(x._21, out)
        wt22.toWire(x._22, out)
      }
      def fromWire(in: DataInput): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22) = {
        val a = wt1.fromWire(in)
        val b = wt2.fromWire(in)
        val c = wt3.fromWire(in)
        val d = wt4.fromWire(in)
        val e = wt5.fromWire(in)
        val f = wt6.fromWire(in)
        val g = wt7.fromWire(in)
        val h = wt8.fromWire(in)
        val i = wt9.fromWire(in)
        val j = wt10.fromWire(in)
        val k = wt11.fromWire(in)
        val l = wt12.fromWire(in)
        val m = wt13.fromWire(in)
        val n = wt14.fromWire(in)
        val o = wt15.fromWire(in)
        val p = wt16.fromWire(in)
        val q = wt17.fromWire(in)
        val r = wt18.fromWire(in)
        val s = wt19.fromWire(in)
        val t = wt20.fromWire(in)
        val u = wt21.fromWire(in)
        val v = wt22.fromWire(in)
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
      }
    }
  /* ------------- END AUTO-GENERATED SECTION 2 ------------- */

  /**
   * Traversable structures
   */
  implicit def TraversableFmt[CC[X] <: Traversable[X], T](implicit wt: WireFormat[T], bf: CanBuildFrom[_, T, CC[T]]) = {
    val builder: Builder[T, CC[T]] = bf()
    new WireFormat[CC[T]] {
      private val b: Builder[T, CC[T]] = builder
      def toWire(x: CC[T], out: DataOutput) = {
        require(x != null, "Cannot serialize a null Traversable. Consider using an empty collection, or a Option[Traversable]")
        out.writeInt(x.size)
        x.foreach { wt.toWire(_, out) }
      }
      def fromWire(in: DataInput): CC[T] = {
        val size = in.readInt()
        builder.clear()
        builder.sizeHint(size)
        for (_ <- 0 to (size - 1)) { b += wt.fromWire(in) }
        b.result()
      }
    }
  }

  /**
   * Map structures
   */
  implicit def MapFmt[CC[X, Y] <: Map[X, Y], K, V](implicit wtK: WireFormat[K], wtV: WireFormat[V], bf: CanBuildFrom[_, (K, V), CC[K, V]]) = {
    val builder: Builder[(K, V), CC[K, V]] = bf()
    new WireFormat[CC[K, V]] {
      private val b: Builder[(K, V), CC[K, V]] = builder
      def toWire(x: CC[K, V], out: DataOutput) = {
        require(x != null, "Cannot serialize a null Map. Consider using an empty collection, or a Option[Map]")
        out.writeInt(x.size)
        x.foreach { case (k, v) => wtK.toWire(k, out); wtV.toWire(v, out) }
      }
      def fromWire(in: DataInput): CC[K, V] = {
        val size = in.readInt()
        builder.clear()
        builder.sizeHint(size)
        for (_ <- 0 to (size - 1)) {
          val k: K = wtK.fromWire(in)
          val v: V = wtV.fromWire(in)
          b += (k -> v)
        }
        b.result()
      }
    }
  }

  /* Arrays */
  implicit def ArrayFmt[T](implicit m: Manifest[T], wt: WireFormat[T]) = new WireFormat[Array[T]] {
    val builder = ArrayBuilder.make()(m)
    def toWire(xs: Array[T], out: DataOutput) = {
      out.writeInt(xs.length)
      xs.foreach { wt.toWire(_, out) }
    }
    def fromWire(in: DataInput): Array[T] = {
      builder.clear()
      val length = in.readInt()
      (0 to (length - 1)).foreach { _ => builder += wt.fromWire(in) }
      builder.result()
    }
  }

  /**
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
}

/**
  * Copyright: [2011] Ben Lever
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


/** Implicit definitions of HadoopWritable instancess for common types. */
object HadoopWritable {

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
    def toWire(x: Int, out: DataOutput) = (new IntWritable(x)).write(out)
    def fromWire(in: DataInput): Int = { val x = new IntWritable; x.readFields(in); x.get }
    def show(x: Int) = x.toString
  }

  implicit def Long = new HadoopWritable[Long] {
    def toWire(x: Long, out: DataOutput) = (new LongWritable(x)).write(out)
    def fromWire(in: DataInput): Long = { val x = new LongWritable; x.readFields(in); x.get }
    def show(x: Long) = x.toString
  }

  implicit def Double = new HadoopWritable[Double] {
    def toWire(x: Double, out: DataOutput) = (new DoubleWritable(x)).write(out)
    def fromWire(in: DataInput): Double = { val x = new DoubleWritable; x.readFields(in); x.get }
    def show(x: Double) = x.toString
  }

  implicit def Char = new HadoopWritable[Char] {
    def toWire(x: Char, out: DataOutput) = (new ByteWritable(x.toByte)).write(out)
    def fromWire(in: DataInput): Char = { val x = new ByteWritable; x.readFields(in); x.get.toChar }
    def show(x: Char) = x.toString
  }

  implicit def Byte = new HadoopWritable[Byte] {
    def toWire(x: Byte, out: DataOutput) = (new ByteWritable(x)).write(out)
    def fromWire(in: DataInput): Byte = { val x = new ByteWritable; x.readFields(in); x.get }
    def show(x: Byte) = x.toString
  }

  implicit def String = new HadoopWritable[String] {
    def toWire(x: String, out: DataOutput) = (new Text(x)).write(out)
    def fromWire(in: DataInput): String = { val x = new Text; x.readFields(in); x.toString }
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

  /*
   * List-like structures
   */
  implicit def Iterable[T](implicit wt: HadoopWritable[T]) = new HadoopWritable[Iterable[T]] {
    def toWire(x: Iterable[T], out: DataOutput) = {
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
}

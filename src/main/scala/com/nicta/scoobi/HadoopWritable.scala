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
//implicit def Any = new HadoopWritable[Any] {
//  def toWire(x: Any, out: DataOutput) = {
//    val bytesOut = new ByteArrayOutputStream
//    val bOut =  new ObjectOutputStream(bytesOut)
//    bOut.writeObject(x)
//    bOut.close()

//    val arr = bytesOut.toByteArray
//    out.writeInt(arr.size)
//    out.write(arr)
//  }

//  def fromWire(in: DataInput): Any = {
//    val size = in.readInt()
//    val barr = new Array[Byte](size)
//    in.readFully(barr)

//    val bIn = new ObjectInputStream(new ByteArrayInputStream(barr))
//    bIn.readObject.asInstanceOf[Any]
//  }
//}

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
  implicit def Tuple2[A, B](implicit wrt1: HadoopWritable[A], wrt2: HadoopWritable[B]) =
    new HadoopWritable[(A, B)] {
      def toWire(x: (A, B), out: DataOutput) = {
        wrt1.toWire(x._1, out)
        wrt2.toWire(x._2, out)
      }
      def fromWire(in: DataInput): (A, B) = {
        val a = wrt1.fromWire(in)
        val b = wrt2.fromWire(in)
        (a, b)
      }
      def show(x: (A, B)) = "(" + List(wrt1.show(x._1), wrt2.show(x._2)).mkString(",") + ")"
  }

  implicit def Tuple3[A, B, C](implicit wrt1: HadoopWritable[A], wrt2: HadoopWritable[B], wrt3: HadoopWritable[C]) =
    new HadoopWritable[(A, B, C)] {
      def toWire(x: (A, B, C), out: DataOutput) = {
        wrt1.toWire(x._1, out)
        wrt2.toWire(x._2, out)
        wrt3.toWire(x._3, out)
      }
      def fromWire(in: DataInput): (A, B, C) = {
        val a = wrt1.fromWire(in)
        val b = wrt2.fromWire(in)
        val c = wrt3.fromWire(in)
        (a, b, c)
      }
      def show(x: (A, B, C)) = "(" + List(wrt1.show(x._1), wrt2.show(x._2), wrt3.show(x._3)).mkString(",") + ")"
  }

  /* List-like structures - TODO */

}

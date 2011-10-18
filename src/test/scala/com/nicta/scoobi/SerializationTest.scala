package com.nicta.scoobi

import org.scalacheck._
import java.io._
import HadoopWritable._

import Arbitrary.arbitrary

object SerializationTest extends Properties("Serializable") {

  def serialize[T : HadoopWritable](obj: T): Array[Byte] = {
    val bs = new ByteArrayOutputStream
    val dos = new DataOutputStream(bs)

    implicitly[HadoopWritable[T]].toWire(obj, dos)

    bs.toByteArray
  }

  def deserialize[T : HadoopWritable](raw: Array[Byte]) : T = {
    val bais = new ByteArrayInputStream(raw)
    val di = new DataInputStream(bais)
    implicitly[HadoopWritable[T]].fromWire(di)
  }

  case class WritableDoubleStringInt(a: Double, b: String, c: Int)
  implicit val DoubleStringIntConversion = allowWritable(WritableDoubleStringInt, WritableDoubleStringInt.unapply _)

  case class DefaultDoubleStringInt(a: Double, b: String, c: Int)

  case class WritableStringNested(a: String, b: WritableDoubleStringInt)
  implicit val WritableStringNestedConversion = allowWritable(WritableStringNested, WritableStringNested.unapply _)

  case class DefaultStringNested(a: String, b: DefaultDoubleStringInt)

  case class IntHolder(a: Int)
  implicit val IntHolderWritable = allowWritable(IntHolder, IntHolder.unapply _)

  val genString = arbitrary[String] suchThat (_ != null)

  val genWritableDoubleStringInt = for {
    d <- arbitrary[Double]
    s <- genString
    i <- arbitrary[Int]
  } yield WritableDoubleStringInt(d, s, i)

  val genDefaultDoubleStringInt = for {
    d <- arbitrary[Double]
    s <- genString
    i <- arbitrary[Int]
  } yield DefaultDoubleStringInt(d, s, i)

  val genWritableStringNested = for {
    s <- genString
    wdsi <- genWritableDoubleStringInt
  } yield WritableStringNested(s, wdsi)

  val genDefaultStringNested = for {
    s <- genString
    ddsi <- genDefaultDoubleStringInt
  } yield DefaultStringNested(s, ddsi)

  val genBothDoubleStringInt = for {
    d <- arbitrary[Double]
    s <- genString
    i <- arbitrary[Int]
  } yield (WritableDoubleStringInt(d, s, i), DefaultDoubleStringInt(d, s, i))

  val genIntHolder = for {
    a <- arbitrary[Int]
  } yield IntHolder(a)


  import Prop.forAll

  def serializesCorrectly[T : HadoopWritable](x: T) : Boolean = deserialize[T](serialize(x)) == x

  property("WritableDoubleStringInt serializes correctly") = forAll(genWritableDoubleStringInt) {
    (x: WritableDoubleStringInt) => serializesCorrectly(x)
  }

  property("DefaultDoubleStringInt serializes correctly") = forAll(genDefaultDoubleStringInt) {
    (x: DefaultDoubleStringInt) => serializesCorrectly(x)
  }

  property("WritableStringNested serializes correctly") = forAll(genWritableStringNested) {
    (x: WritableStringNested) => serializesCorrectly(x)
  }

  property("DefaultStringNested serializes correctly") = forAll(genDefaultStringNested) {
    (x: DefaultStringNested) =>  serializesCorrectly(x)
  }

  property("WritableDoubleStringInt is (much) more efficient") = forAll(genBothDoubleStringInt) {
    (x: (WritableDoubleStringInt, DefaultDoubleStringInt)) =>
      serialize(x._1).length < serialize(x._2).length
  }

  property("Single field case class can serialize") = forAll(genIntHolder) {
    (x: IntHolder) => serializesCorrectly(x)
  }
}


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

import org.scalacheck._
import java.io._
import WireFormat._

import Arbitrary.arbitrary

object SerializationTest extends Properties("Serializable") {

  def serialize[T : WireFormat](obj: T): Array[Byte] = {
    val bs = new ByteArrayOutputStream
    val dos = new DataOutputStream(bs)

    implicitly[WireFormat[T]].toWire(obj, dos)

    bs.toByteArray
  }

  def deserialize[T : WireFormat](raw: Array[Byte]) : T = {
    val bais = new ByteArrayInputStream(raw)
    val di = new DataInputStream(bais)
    implicitly[WireFormat[T]].fromWire(di)
  }

  case class WritableDoubleStringInt(a: Double, b: String, c: Int)
  implicit val DoubleStringIntConversion = mkCaseWireFormat(WritableDoubleStringInt, WritableDoubleStringInt.unapply _)

  case class DefaultDoubleStringInt(a: Double, b: String, c: Int)

  case class WritableStringNested(a: String, b: WritableDoubleStringInt)
  implicit val WritableStringNestedConversion = mkCaseWireFormat(WritableStringNested, WritableStringNested.unapply _)

  case class DefaultStringNested(a: String, b: DefaultDoubleStringInt)

  case class IntHolder(a: Int)
  implicit val IntHolderWritable = mkCaseWireFormat(IntHolder, IntHolder.unapply _)

  abstract sealed class Base()
  case class SubA(v: Int) extends Base

  implicit val SubAWritable = mkCaseWireFormat(SubA, SubA.unapply _)

  case class SubB(v: String) extends Base
  implicit val SubBWritable = mkCaseWireFormat(SubB, SubB.unapply _)

  object SubC extends Base
  implicit val SubCWritable = mkObjectWireFormat(SubC)

  implicit val BaseWritable = mkAbstractWireFormat[Base, SubA, SubB, SubC.type]

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

  def serializesCorrectly[T : WireFormat](x: T) : Boolean = deserialize[T](serialize(x)) == x

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

  property("ADTs serialize properly") = forAll(genString) {
    (x: String) => serializesCorrectly[Base](SubB(x))
  }

  property("ADTs serialize properly") = forAll(arbitrary[Int]) {
    (x: Int) => serializesCorrectly[Base](SubA(x))
  }

  assert( serializesCorrectly[Base](SubC) )

}


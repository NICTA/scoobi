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

import io.NullDataOutput
import java.io._
import org.specs2.ScalaCheck
import org.scalacheck.{Prop, Arbitrary}
import Arbitrary._
import Prop.forAll
import testing.mutable.UnitSpecification
import WireFormat._
import data._
import com.nicta.scoobi.testing.mutable.UnitSpecification
import com.nicta.scoobi.data._
import com.nicta.scoobi.io.NullDataOutput
import com.nicta.scoobi.data.DefaultStringNested
import com.nicta.scoobi.data.DefaultDoubleStringInt
import com.nicta.scoobi.data.IntHolder

@SuppressWarnings(Array("slow"))
class WireFormatSpec extends UnitSpecification with ScalaCheck with CaseClassData {

  "**Basic types**".p

  "A WireFormat instance is available for basic types" >> {
    serializationIsOkFor[Int]
    serializationIsOkFor[java.lang.Integer]
    serializationIsOkFor[Boolean]
    serializationIsOkFor[Long]
    serializationIsOkFor[Double]
    serializationIsOkFor[Float]
    serializationIsOkFor[Char]
    serializationIsOkFor[Byte]
    serializationIsOkFor[Option[Int]]
    serializationIsOkFor[Either[String, Boolean]]
    serializationIsOkFor[java.util.Date]
    serializationIsOkFor[(Int, String)]
    serializationIsOkFor[(Int, String, Long, Boolean, Double, Float, String, Byte)]
    serializationIsOkFor[Seq[Int]]
    "but a null traversable cannot be serialised" >> {
      implicitly[WireFormat[Seq[Int]]].toWire(null, NullDataOutput) must throwAn[IllegalArgumentException]
    } lt;
    serializationIsOkFor[Map[Int, String]]
    "but a null map cannot be serialised" >> {
      implicitly[WireFormat[Map[Int, String]]].toWire(null, NullDataOutput) must throwAn[IllegalArgumentException]
    } lt;
    serializationIsOkFor[Array[Int]]
  }

  "**Case classes**".newbr

  "A WireFormat instance can be created, using the Serializable interface" >> {
    "for a case class with a Double, a String, an Int" >> {
      // we need to explicitly import the wireformat
      implicit val wf = AnythingFmt[DefaultDoubleStringInt]
      serializationIsOkFor[DefaultDoubleStringInt]
    }
    "for a nested case class" >> {
      // we need to explicitly import the wireformat
      implicit val wf = AnythingFmt[DefaultStringNested]
      serializationIsOkFor[DefaultStringNested]
    }
  }

  "The mkCaseWireFormat method creates WireFormat instances for case classes" >> {

    "for a case class holding a simple Int value" >> {
      implicit val IntHolderWireFormat = mkCaseWireFormat(IntHolder, IntHolder.unapply _)
      serializationIsOkFor[IntHolder]
    }
    "for a case class with 2 Strings and an Int" >> {
      implicit val DoubleStringIntWireFormat = mkCaseWireFormat(WritableDoubleStringInt, WritableDoubleStringInt.unapply _)
      serializationIsOkFor[WritableDoubleStringInt]

      "this method is (much) more efficient than the default one" >> prop {x: WritableDoubleStringInt =>
        serialise(x).length must be_<(serialise(x.toDefault)(AnythingFmt[DefaultDoubleStringInt]).length)
      }
    }
    "for a nested case class" >> {
      implicit val DoubleStringIntWireFormat      = mkCaseWireFormat(WritableDoubleStringInt, WritableDoubleStringInt.unapply _)
      implicit val WritableStringNestedWireFormat = mkCaseWireFormat(WritableStringNested, WritableStringNested.unapply _)

      serializationIsOkFor[WritableStringNested]

      "this method is (much) more efficient than the default one" >> prop { x: WritableStringNested =>
          serialise(x).length must be_<(serialise(x.toDefault)(AnythingFmt[DefaultStringNested]).length)
      }
    }

    "for case classes of an ADT, by making WireFormats for each node type,\n " +
      "including the base type, up to 8 different types" >> {
      implicit val BaseIntWritable    = mkCaseWireFormat(BaseInt, BaseInt.unapply _)
      implicit val BaseStringWritable = mkCaseWireFormat(BaseString, BaseString.unapply _)
      implicit val BaseObjectWritable = mkObjectWireFormat(BaseObject)
      implicit val BaseWritable       = mkAbstractWireFormat[Base, BaseInt, BaseString, BaseObject.type]

      "for one node type" >> prop {
        (s: String) =>
          serializationIsOkWith(BaseString(s))
      }
      "for another node type" >> prop {
        (i: Int) =>
          serializationIsOkWith(BaseInt(i))
      }
      "for an object of the base type" >> prop {
        (i: Int) =>
          serializationIsOkWith(BaseObject)
      }
    }

  }

  def serializationIsOkFor[T: WireFormat : Arbitrary : Manifest] =
    implicitly[Manifest[T]].erasure.getSimpleName + " serialises correctly" >>
      forAll(implicitly[Arbitrary[T]].arbitrary)((t: T) => serializationIsOkWith[T](t))

  def serializationIsOkWith[T: WireFormat](x: T): Boolean = deserialise[T](serialise(x)) must_== x

  def serialise[T: WireFormat](obj: T): Array[Byte] = {
    val bs = new ByteArrayOutputStream
    implicitly[WireFormat[T]].toWire(obj, new DataOutputStream(bs))
    bs.toByteArray
  }

  def deserialise[T: WireFormat](raw: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(raw)
    implicitly[WireFormat[T]].fromWire(new DataInputStream(bais))
  }

}


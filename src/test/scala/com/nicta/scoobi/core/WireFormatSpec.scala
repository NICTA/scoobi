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
import org.apache.avro.generic.GenericRecord
import org.specs2.ScalaCheck
import org.scalacheck.{Prop, Arbitrary}
import Arbitrary._
import Prop.forAll

import testing.mutable.UnitSpecification
import io.NullDataOutput
import data._
import WireFormat._

@SuppressWarnings(Array("slow"))
class WireFormatSpec extends UnitSpecification with ScalaCheck with CaseClassData {

  "**Basic types**".p

  "A WireFormat instance is available for basic types" >> {
    serialisationIsOkFor[Int]
    serialisationIsOkFor[java.lang.Integer]
    serialisationIsOkFor[Boolean]
    serialisationIsOkFor[Long]
    serialisationIsOkFor[Double]
    serialisationIsOkFor[Float]
    serialisationIsOkFor[Char]
    serialisationIsOkFor[Byte]
    serialisationIsOkFor[Option[Int]]
    serialisationIsOkFor[Either[String, Boolean]]
    serialisationIsOkFor[java.util.Date]
    serialisationIsOkFor[(Int, String)]
    serialisationIsOkFor[(Int, String, Long, Boolean, Double, Float, String, Byte)]
    serialisationIsOkFor[Seq[Int]]
    "but a null traversable cannot be serialised" >> {
      implicitly[WireFormat[Seq[Int]]].toWire(null, NullDataOutput) must throwAn[IllegalArgumentException]
    } lt;
    serialisationIsOkFor[Map[Int, String]]
    "but a null map cannot be serialised" >> {
      implicitly[WireFormat[Map[Int, String]]].toWire(null, NullDataOutput) must throwAn[IllegalArgumentException]
    } lt;
    serialisationIsOkFor[Array[Int]]
  }

  "**Case classes**".newbr

  "A WireFormat instance can be created, using the Serializable interface" >> {
    "for a case class with a Double, a String, an Int" >> {
      // we need to explicitly import the wireformat
      implicit val wf = AnythingFmt[DefaultDoubleStringInt]
      serialisationIsOkFor[DefaultDoubleStringInt]
    }
    "for a nested case class" >> {
      // we need to explicitly import the wireformat
      implicit val wf = AnythingFmt[DefaultStringNested]
      serialisationIsOkFor[DefaultStringNested]
    }
  }

  "The mkCaseWireFormat method creates WireFormat instances for case classes" >> {

    "for a case class holding a simple Int value" >> {
      implicit val IntHolderWireFormat = mkCaseWireFormat(IntHolder, IntHolder.unapply _)
      serialisationIsOkFor[IntHolder]
    }
    "for a case class with 2 Strings and an Int" >> {
      implicit val DoubleStringIntWireFormat = mkCaseWireFormat(WritableDoubleStringInt, WritableDoubleStringInt.unapply _)
      serialisationIsOkFor[WritableDoubleStringInt]

      "this method is (much) more efficient than the default one" >> prop {x: WritableDoubleStringInt =>
        serialise(x).length must be_<(serialise(x.toDefault)(AnythingFmt[DefaultDoubleStringInt]).length)
      }
    }
    "for a nested case class" >> {
      implicit val DoubleStringIntWireFormat      = mkCaseWireFormat(WritableDoubleStringInt, WritableDoubleStringInt.unapply _)
      implicit val WritableStringNestedWireFormat = mkCaseWireFormat(WritableStringNested, WritableStringNested.unapply _)

      serialisationIsOkFor[WritableStringNested]

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
          serialisationIsOkWith(BaseString(s))
      }
      "for another node type" >> prop {
        (i: Int) =>
          serialisationIsOkWith(BaseInt(i))
      }
      "for an object of the base type" >> prop {
        (i: Int) =>
          serialisationIsOkWith(BaseObject)
      }
    }

  }

  "**Avro**".newbr

  "A WireFormat is available" >> {
    import com.nicta.scoobi.testavroschema._

    "for avro SpecificRecord type" >> {
      implicit val wireformat: WireFormat[SampleRecord] = AvroFmt
      val fixed = new SampleRecord("foo", 42, new Sha1(Array.fill(20)(0xAA.toByte)))
      serialisationIsOkWith(fixed)(wireformat)
    }

    "for avro SpecificFixed type" >> {
      val fixed = new Sha1(Array.fill(20)(0xAA.toByte))
      serialisationIsOkWith(fixed)
    }
  }

  def serialisationIsOkFor[T : WireFormat : Manifest : Arbitrary] =
    implicitly[Manifest[T]].runtimeClass.getSimpleName + " serialises correctly" >>
      forAll(implicitly[Arbitrary[T]].arbitrary)((t: T) => serialisationIsOkWith[T](t))

  def serialisationIsOkWith[T: WireFormat](x: T): Boolean = deserialise[T](serialise(x)) must_== x

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


package com.nicta.scoobi

import io.NullDataOutput
import java.io._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck.{Prop, Arbitrary, Gen}
import Arbitrary._
import Gen._
import Prop.forAll
import WireFormat._
import data._
import com.nicta.scoobi.testing.mutable.Unit

@SuppressWarnings(Array("slow"))
class WireFormatSpec extends Specification with ScalaCheck with CaseClassData with Unit {

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
    serializationIsOkFor[String]
    "but a null string cannot be serialized" >> {
      implicitly[WireFormat[String]].toWire(null, NullDataOutput) must throwAn[IllegalArgumentException]
    } lt;
    serializationIsOkFor[Seq[Int]]
    "but a null traversable cannot be serialized" >> {
      implicitly[WireFormat[Seq[Int]]].toWire(null, NullDataOutput) must throwAn[IllegalArgumentException]
    } lt;
    serializationIsOkFor[Map[Int, String]]
    "but a null map cannot be serialized" >> {
      implicitly[WireFormat[Map[Int, String]]].toWire(null, NullDataOutput) must throwAn[IllegalArgumentException]
    } lt;
    serializationIsOkFor[Array[Int]]
  }

  "**Case classes**".newbr

  "A WireFormat instance can be created, using the Serializable interface" >> {
    "for a case class with a Double, a String, an Int" >> {
      serializationIsOkFor[DefaultDoubleStringInt]
    }
    "for a nested case class" >> {
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

      "this method is (much) more efficient than the default one" >> check { x: WritableDoubleStringInt =>
        serialize(x).length must be_<(serialize(x.toDefault).length)
      }
    }
    "for a nested case class" >> {
      implicit val WritableStringNestedWireFormat = mkCaseWireFormat(WritableStringNested, WritableStringNested.unapply _)
      serializationIsOkFor[WritableStringNested]

      "this method is (much) more efficient than the default one" >> check { x: WritableStringNested =>
        serialize(x).length must be_<(serialize(x.toDefault).length)
      }
    }

    "for case classes of an ADT, by making WireFormats for each node type,\n "+
    "including the base type, up to 8 different types" >> {
      implicit val BaseIntWritable    = mkCaseWireFormat(BaseInt, BaseInt.unapply _)
      implicit val BaseStringWritable = mkCaseWireFormat(BaseString, BaseString.unapply _)
      implicit val BaseObjectWritable = mkObjectWireFormat(BaseObject)
      implicit val BaseWritable       = mkAbstractWireFormat[Base, BaseInt, BaseString, BaseObject.type]

      "for one node type" >> check { (s: String) =>
        serializationIsOkWith(BaseString(s))
      }
      "for another node type" >> check { (i: Int) =>
        serializationIsOkWith(BaseInt(i))
      }
      "for an object of the base type" >> check { (i: Int) =>
        serializationIsOkWith(BaseObject)
      }
    }

  }

  def serializationIsOkFor[T : WireFormat : Arbitrary : Manifest] =
    implicitly[Manifest[T]].erasure.getSimpleName+" serializes correctly" >>
      forAll(implicitly[Arbitrary[T]].arbitrary)((t: T) => serializationIsOkWith[T](t))

  def serializationIsOkWith[T : WireFormat](x: T) : Boolean = deserialize[T](serialize(x)) must_== x

  def serialize[T : WireFormat](obj: T): Array[Byte] = {
    val bs = new ByteArrayOutputStream
    implicitly[WireFormat[T]].toWire(obj, new DataOutputStream(bs))
    bs.toByteArray
  }

  def deserialize[T : WireFormat](raw: Array[Byte]) : T = {
    val bais = new ByteArrayInputStream(raw)
    implicitly[WireFormat[T]].fromWire(new DataInputStream(bais))
  }

}


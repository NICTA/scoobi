package com.nicta.scoobi

import java.io._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck.Arbitrary._
import org.scalacheck.{Prop, Arbitrary, Gen}
import Prop.forAll
import com.nicta.scoobi.WireFormat._

@SuppressWarnings(Array("slow"))
class WireFormatSpec extends Specification with ScalaCheck with CaseClassData {

  "A WireFormat instance can be automatically created, using the Serializable interface of a case class" >> {
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

    "for case classes of an ADT, by making WireFormats for each node type,\n including the base type" >> {
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

  def serializationIsOkFor[T : WireFormat : Gen : Manifest] =
    implicitly[Manifest[T]].erasure.getSimpleName+" serializes correctly" >>
      forAll(implicitly[Gen[T]])((t: T) => serializationIsOkWith[T](t))

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

/**
 * case classes generators
 */
trait CaseClassData {

  /**
   * Generators
   */
  val genString = arbitrary[String] suchThat (_ != null)

  implicit val genIntHolder: Gen[IntHolder] = arbitrary[Int].map(IntHolder)
  implicit def arbitraryOf[T: Gen]: Arbitrary[T] = Arbitrary(implicitly[Gen[T]])
  implicit val genWritableDoubleStringInt: Gen[WritableDoubleStringInt] = for {
    d <- arbitrary[Double]
    s <- genString
    i <- arbitrary[Int]
  } yield WritableDoubleStringInt(d, s, i)

  implicit val genDefaultDoubleStringInt: Gen[DefaultDoubleStringInt] = for {
    d <- arbitrary[Double]
    s <- genString
    i <- arbitrary[Int]
  } yield DefaultDoubleStringInt(d, s, i)

  implicit val genWritableStringNested: Gen[WritableStringNested] = for {
    s <- genString
    wdsi <- genWritableDoubleStringInt
  } yield WritableStringNested(s, wdsi)

  implicit val genDefaultStringNested: Gen[DefaultStringNested] = for {
    s <- genString
    ddsi <- genDefaultDoubleStringInt
  } yield DefaultStringNested(s, ddsi)

}
case class IntHolder(a: Int)

case class DefaultDoubleStringInt(a: Double, b: String, c: Int)
case class WritableDoubleStringInt(a: Double, b: String, c: Int) {
  def toDefault = DefaultDoubleStringInt(a, b, c)
}

case class DefaultStringNested(a: String, b: DefaultDoubleStringInt)
case class WritableStringNested(a: String, b: WritableDoubleStringInt) {
  def toDefault = DefaultStringNested(a, b.toDefault)
}

sealed trait Base
case class BaseInt(v: Int) extends Base
case class BaseString(v: String) extends Base
object BaseObject extends Base

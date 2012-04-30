package com.nicta.scoobi.data

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._
import scalaz.Scalaz._

/**
 * case classes generators
 */
trait CaseClassData extends Data {
  import org.scalacheck.util.Buildable._

  implicit def arbitraryOf[T: Gen]: Arbitrary[T] = Arbitrary(implicitly[Gen[T]])
  def genOf[T: Arbitrary]: Gen[T] = implicitly[Arbitrary[T]].arbitrary

  implicit def arbitraryInteger: Arbitrary[java.lang.Integer] =
    Arbitrary(implicitly[Arbitrary[Int]].arbitrary.map(i => new java.lang.Integer(i)))

  implicit def arbitrarySeq[T : Arbitrary]: Arbitrary[Seq[T]] =
    Arbitrary(containerOf1(implicitly[Arbitrary[T]].arbitrary)(buildableList[T]))

  /**
   * Generators
   */
  implicit val genIntHolder: Gen[IntHolder] = arbitrary[Int].map(IntHolder)

  implicit val genWritableDoubleStringInt: Gen[WritableDoubleStringInt] =
    (arbitrary[Double] |@| nonNullString |@| arbitrary[Int]) (WritableDoubleStringInt)

  implicit val genDefaultDoubleStringInt: Gen[DefaultDoubleStringInt] =
    (arbitrary[Double] |@| nonNullString |@| arbitrary[Int])(DefaultDoubleStringInt)

  implicit val genWritableStringNested: Gen[WritableStringNested] =
    (nonNullString |@| genWritableDoubleStringInt)(WritableStringNested)

  implicit val genDefaultStringNested: Gen[DefaultStringNested] =
    (nonNullString |@| genDefaultDoubleStringInt)(DefaultStringNested)

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


package com.nicta.scoobi
package data

import scalaz.Apply
import org.scalacheck.Gen
import org.scalacheck.Arbitrary._

/**
 * Generic data functions
 */
trait Data {

  /**
   * allow the use of the applicative syntax to build generators:
   *
   * (gen1 |@| gen2 |@| gen3)(function(_,_,_))
   *
   * It's especially useful when building generators for case classes
   *
   * import scalaz.Scalaz._
   * case class MyClass(a: A, b: B, c: C)
   *
   * val MyCaseClassGenerator: Gen[MyCaseClass] = (genA |@| genB |@| genC)(MyCaseClass)
   *
   */
  implicit def genIsApply: Apply[Gen] = new Apply[Gen] {
    def ap[A, B](fa: => Gen[A])(f: => Gen[(A) => B]) = fa.map2(f)((v, function) => function(v))
    def map[A, B](fa: Gen[A])(f: (A) => B) = fa map f
  }

  lazy val nonNullString = arbitrary[String] suchThat (_ != null)

}


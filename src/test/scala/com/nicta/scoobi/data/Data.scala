package com.nicta.scoobi
package data

import scalaz.Apply
import org.scalacheck.Gen
import org.scalacheck.Arbitrary._
import Gen._
import scalaz.Scalaz._

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

  /**
   * @return a new generator memoizing the previous values and returning old ones x % of the time
   *         this allows to share existing generated data between other generators
   */
  def memo[T](g: Gen[T], ratio: Int = 30): Gen[T] = {
    lazy val previousValues = new scala.collection.mutable.HashSet[T]
    def memoizeValue(v: T) = { previousValues.add(v); v }
    for {
      v           <- g
      usePrevious <- choose(0, 100)
      n           <- choose(0, previousValues.size)
    } yield {
      if (usePrevious <= ratio && previousValues.nonEmpty)
        if (n == previousValues.size) previousValues.toList(n - 1) else previousValues.toList(n)
      else memoizeValue(v)
    }
  }

  def distinctPairs[T1 <: AnyRef](seq1: Seq[T1]): Seq[(T1, T1)]                    = distinctPairs(seq1, seq1)
  def distinctPairs[T1 <: AnyRef, T2  <: AnyRef](seq1: Seq[T1], seq2: Seq[T2]): Seq[(T1, T2)] = (seq1.toList |@| seq2.toList)((_,_)).filterNot(p => p._1 eq p._2).distinct.toSeq

}


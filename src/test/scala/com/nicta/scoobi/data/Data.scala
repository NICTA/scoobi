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
package data

import org.scalacheck.Gen
import org.scalacheck.Arbitrary._
import Gen._
import scalaz.Apply
import scalaz.Scalaz._

/**
 * Generic data functions
 */
trait Data { outer =>

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
    def ap[A, B](fa: => Gen[A])(fab: => Gen[(A) => B]) = for {
      a <- fa
      f <- fab
    } yield f(a)

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

  implicit def memoize[T](g: Gen[T]): MemoizedGen[T] = new MemoizedGen[T](g)
  class MemoizedGen[T](g: Gen[T]) {
    def memo =  outer.memo(g)
    def memo(ratio: Int) = outer.memo(g, ratio)
  }

  def distinctPairs[T1 <: AnyRef](set1: Set[T1]): Set[(T1, T1)] = distinctPairs(set1, set1)
  def distinctPairs[T1 <: AnyRef, T2  <: AnyRef](set1: Set[T1], set2: Set[T2]): Set[(T1, T2)] =
    ^(set1.toList, set2.toList)((_,_)).filterNot(p => p._1 eq p._2).toSet

}


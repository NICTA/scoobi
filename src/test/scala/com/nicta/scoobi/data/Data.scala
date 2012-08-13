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


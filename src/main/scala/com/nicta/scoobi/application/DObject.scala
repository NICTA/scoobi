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
package application

import core._
import impl.plan.DObjectImpl

/** This object provides a set of operations to create distributed objects. */
object DObjects extends DObjects {

  /** Create a new distributed list object from an "ordinary" value. */
  def apply[A : WireFormat](x: A): DObject[A] = DObjectImpl(x)
}

/** Implicit conversions for DObjects */
trait DObjects {
  /* Implicit conversions from tuples of DObjects to DObject tuples. */
  implicit def tupled2[T1 : WireFormat,
  T2 : WireFormat]
  (tup: (DObject[T1], DObject[T2])): DObject[(T1, T2)] = DObjectImpl.tupled2(tup)

  implicit def tupled3[T1 : WireFormat,
  T2 : WireFormat,
  T3 : WireFormat]
  (tup: (DObject[T1], DObject[T2], DObject[T3])): DObject[(T1, T2, T3)] =
    tupled2(tup._1, tupled2(tup._2, tup._3)) map { case (a, (b, c)) => (a, b, c) }

  implicit def tupled4[T1 : WireFormat,
  T2 : WireFormat,
  T3 : WireFormat,
  T4 : WireFormat]
  (tup: (DObject[T1], DObject[T2], DObject[T3], DObject[T4])): DObject[(T1, T2, T3, T4)] =
    tupled2(tup._1, tupled3(tup._2, tup._3, tup._4)) map { case (a, (b, c, d)) => (a, b, c, d) }

  implicit def tupled5[T1 : WireFormat,
  T2 : WireFormat,
  T3 : WireFormat,
  T4 : WireFormat,
  T5 : WireFormat]
  (tup: (DObject[T1], DObject[T2], DObject[T3], DObject[T4], DObject[T5])): DObject[(T1, T2, T3, T4, T5)] =
    tupled2(tup._1, tupled4(tup._2, tup._3, tup._4, tup._5)) map { case (a, (b, c, d, e)) => (a, b, c, d, e) }

  implicit def tupled6[T1 : WireFormat,
  T2 : WireFormat,
  T3 : WireFormat,
  T4 : WireFormat,
  T5 : WireFormat,
  T6 : WireFormat]
  (tup: (DObject[T1], DObject[T2], DObject[T3], DObject[T4], DObject[T5], DObject[T6])): DObject[(T1, T2, T3, T4, T5, T6)] =
    tupled2(tup._1, tupled5(tup._2, tup._3, tup._4, tup._5, tup._6)) map { case (a, (b, c, d, e, f)) => (a, b, c, d, e, f) }

  implicit def tupled7[T1 : WireFormat,
  T2 : WireFormat,
  T3 : WireFormat,
  T4 : WireFormat,
  T5 : WireFormat,
  T6 : WireFormat,
  T7 : WireFormat]
  (tup: (DObject[T1], DObject[T2], DObject[T3], DObject[T4], DObject[T5], DObject[T6], DObject[T7])): DObject[(T1, T2, T3, T4, T5, T6, T7)] =
    tupled2(tup._1, tupled6(tup._2, tup._3, tup._4, tup._5, tup._6, tup._7)) map { case (a, (b, c, d, e, f, g)) => (a, b, c, d, e, f, g) }

  implicit def tupled8[T1 : WireFormat,
  T2 : WireFormat,
  T3 : WireFormat,
  T4 : WireFormat,
  T5 : WireFormat,
  T6 : WireFormat,
  T7 : WireFormat,
  T8 : WireFormat]
  (tup: (DObject[T1], DObject[T2], DObject[T3], DObject[T4], DObject[T5], DObject[T6], DObject[T7], DObject[T8])): DObject[(T1, T2, T3, T4, T5, T6, T7, T8)] =
    tupled2(tup._1, tupled7(tup._2, tup._3, tup._4, tup._5, tup._6, tup._7, tup._8)) map { case (a, (b, c, d, e, f, g, h)) => (a, b, c, d, e, f, g, h) }
}

/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.lib

import java.io._
import scala.collection.immutable.VectorBuilder

import com.nicta.scoobi.DList
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.WireFormat._
import com.nicta.scoobi.lib.Multi._


object Join {

  /** Perform an inner-join of two (2) distributed lists using a specified join-predicate. */
  def joinOn[K  : Manifest : WireFormat,
             T  : Manifest : WireFormat : Ordering,
             A1 : Manifest : WireFormat,
             A2 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)])
      (jp: K => T)
    : DList[(A1, A2)] = {

    val d1s: DList[(T, Multi[A1, A2, A1, A1, A1, A1, A1, A1])] = d1 map { case (k, a1) => (jp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A1, A1, A1, A1, A1, A1])] = d2 map { case (k, a2) => (jp(k), Some2(a2)) }

    val joined = (d1s ++ d2s).groupByKey flatMap { case (_, as) =>
      val vb1: VectorBuilder[A1] = new VectorBuilder[A1]()
      val vb2: VectorBuilder[A2] = new VectorBuilder[A2]()
      as foreach {
        case Some1(a1) => vb1 += a1
        case Some2(a2) => vb2 += a2
      }
      for (a1 <- vb1.result(); a2 <- vb2.result())
        yield (a1, a2)
    }

    joined
  }

  /** Perform an equijoin of two (2) distributed lists. */
  def join[K  : Manifest : WireFormat : Ordering,
           A1 : Manifest : WireFormat,
           A2 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)])
    : DList[(A1, A2)] = joinOn(d1, d2)(identity)


  /** Perform an inner-join of three (3) distributed lists using a specified join-predicate. */
  def joinOn[K  : Manifest : WireFormat,
             T  : Manifest : WireFormat : Ordering,
             A1 : Manifest : WireFormat,
             A2 : Manifest : WireFormat,
             A3 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)])
      (jp: K => T)
    : DList[(A1, A2, A3)] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A1, A1, A1, A1, A1])] = d1 map { case (k, a1) => (jp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A1, A1, A1, A1, A1])] = d2 map { case (k, a2) => (jp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A1, A1, A1, A1, A1])] = d3 map { case (k, a3) => (jp(k), Some3(a3)) }

    val joined = (d1s ++ d2s ++ d3s).groupByKey flatMap { case (_, as) =>
      val vb1: VectorBuilder[A1] = new VectorBuilder[A1]()
      val vb2: VectorBuilder[A2] = new VectorBuilder[A2]()
      val vb3: VectorBuilder[A3] = new VectorBuilder[A3]()
      as foreach {
        case Some1(a1) => vb1 += a1
        case Some2(a2) => vb2 += a2
        case Some3(a3) => vb3 += a3
      }
      for (a1 <- vb1.result(); a2 <- vb2.result(); a3 <- vb3.result())
        yield (a1, a2, a3)
    }

    joined
  }

  /** Perform an equijoin of three (3) distributed lists. */
  def join[K  : Manifest : WireFormat : Ordering,
           A1 : Manifest : WireFormat,
           A2 : Manifest : WireFormat,
           A3 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)])
    : DList[(A1, A2, A3)] = joinOn(d1, d2, d3)(identity)


  /** Perform an inner-join of four (4) distributed lists using a specified join-predicate. */
  def joinOn[K  : Manifest : WireFormat,
             T  : Manifest : WireFormat : Ordering,
             A1 : Manifest : WireFormat,
             A2 : Manifest : WireFormat,
             A3 : Manifest : WireFormat,
             A4 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)])
      (jp: K => T)
    : DList[(A1, A2, A3, A4)] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A4, A1, A1, A1, A1])] = d1 map { case (k, a1) => (jp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A4, A1, A1, A1, A1])] = d2 map { case (k, a2) => (jp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A4, A1, A1, A1, A1])] = d3 map { case (k, a3) => (jp(k), Some3(a3)) }
    val d4s: DList[(T, Multi[A1, A2, A3, A4, A1, A1, A1, A1])] = d4 map { case (k, a4) => (jp(k), Some4(a4)) }

    val joined = (d1s ++ d2s ++ d3s ++ d4s).groupByKey flatMap { case (_, as) =>
      val vb1: VectorBuilder[A1] = new VectorBuilder[A1]()
      val vb2: VectorBuilder[A2] = new VectorBuilder[A2]()
      val vb3: VectorBuilder[A3] = new VectorBuilder[A3]()
      val vb4: VectorBuilder[A4] = new VectorBuilder[A4]()
      as foreach {
        case Some1(a1) => vb1 += a1
        case Some2(a2) => vb2 += a2
        case Some3(a3) => vb3 += a3
        case Some4(a4) => vb4 += a4
      }
      for (a1 <- vb1.result(); a2 <- vb2.result(); a3 <- vb3.result(); a4 <- vb4.result())
        yield (a1, a2, a3, a4)
    }

    joined
  }

  /** Perform an equijoin of four (4) distributed lists. */
  def join[K  : Manifest : WireFormat : Ordering,
           A1 : Manifest : WireFormat,
           A2 : Manifest : WireFormat,
           A3 : Manifest : WireFormat,
           A4 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)])
    : DList[(A1, A2, A3, A4)] = joinOn(d1, d2, d3, d4)(identity)


  /** Perform an inner-join of five (5) distributed lists using a specified join-predicate. */
  def joinOn[K  : Manifest : WireFormat,
             T  : Manifest : WireFormat : Ordering,
             A1 : Manifest : WireFormat,
             A2 : Manifest : WireFormat,
             A3 : Manifest : WireFormat,
             A4 : Manifest : WireFormat,
             A5 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)])
      (jp: K => T)
    : DList[(A1, A2, A3, A4, A5)] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A4, A5, A1, A1, A1])] = d1 map { case (k, a1) => (jp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A4, A5, A1, A1, A1])] = d2 map { case (k, a2) => (jp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A4, A5, A1, A1, A1])] = d3 map { case (k, a3) => (jp(k), Some3(a3)) }
    val d4s: DList[(T, Multi[A1, A2, A3, A4, A5, A1, A1, A1])] = d4 map { case (k, a4) => (jp(k), Some4(a4)) }
    val d5s: DList[(T, Multi[A1, A2, A3, A4, A5, A1, A1, A1])] = d5 map { case (k, a5) => (jp(k), Some5(a5)) }

    val joined = (d1s ++ d2s ++ d3s ++ d4s ++ d5s).groupByKey flatMap { case (_, as) =>
      val vb1: VectorBuilder[A1] = new VectorBuilder[A1]()
      val vb2: VectorBuilder[A2] = new VectorBuilder[A2]()
      val vb3: VectorBuilder[A3] = new VectorBuilder[A3]()
      val vb4: VectorBuilder[A4] = new VectorBuilder[A4]()
      val vb5: VectorBuilder[A5] = new VectorBuilder[A5]()
      as foreach {
        case Some1(a1) => vb1 += a1
        case Some2(a2) => vb2 += a2
        case Some3(a3) => vb3 += a3
        case Some4(a4) => vb4 += a4
        case Some5(a5) => vb5 += a5
      }
      for (a1 <- vb1.result(); a2 <- vb2.result(); a3 <- vb3.result(); a4 <- vb4.result(); a5 <- vb5.result())
        yield (a1, a2, a3, a4, a5)
    }

    joined
  }

  /** Perform an equijoin of five (5) distributed lists. */
  def join[K  : Manifest : WireFormat : Ordering,
           A1 : Manifest : WireFormat,
           A2 : Manifest : WireFormat,
           A3 : Manifest : WireFormat,
           A4 : Manifest : WireFormat,
           A5 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)])
    : DList[(A1, A2, A3, A4, A5)] = joinOn(d1, d2, d3, d4, d5)(identity)


  /** Perform an inner-join of six (6) distributed lists using a specified join-predicate. */
  def joinOn[K  : Manifest : WireFormat,
             T  : Manifest : WireFormat : Ordering,
             A1 : Manifest : WireFormat,
             A2 : Manifest : WireFormat,
             A3 : Manifest : WireFormat,
             A4 : Manifest : WireFormat,
             A5 : Manifest : WireFormat,
             A6 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)])
      (jp: K => T)
    : DList[(A1, A2, A3, A4, A5, A6)] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d1 map { case (k, a1) => (jp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d2 map { case (k, a2) => (jp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d3 map { case (k, a3) => (jp(k), Some3(a3)) }
    val d4s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d4 map { case (k, a4) => (jp(k), Some4(a4)) }
    val d5s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d5 map { case (k, a5) => (jp(k), Some5(a5)) }
    val d6s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d6 map { case (k, a6) => (jp(k), Some6(a6)) }

    val joined = (d1s ++ d2s ++ d3s ++ d4s ++ d5s ++ d6s).groupByKey flatMap { case (_, as) =>
      val vb1: VectorBuilder[A1] = new VectorBuilder[A1]()
      val vb2: VectorBuilder[A2] = new VectorBuilder[A2]()
      val vb3: VectorBuilder[A3] = new VectorBuilder[A3]()
      val vb4: VectorBuilder[A4] = new VectorBuilder[A4]()
      val vb5: VectorBuilder[A5] = new VectorBuilder[A5]()
      val vb6: VectorBuilder[A6] = new VectorBuilder[A6]()
      as foreach {
        case Some1(a1) => vb1 += a1
        case Some2(a2) => vb2 += a2
        case Some3(a3) => vb3 += a3
        case Some4(a4) => vb4 += a4
        case Some5(a5) => vb5 += a5
        case Some6(a6) => vb6 += a6
      }
      for (a1 <- vb1.result(); a2 <- vb2.result(); a3 <- vb3.result(); a4 <- vb4.result(); a5 <- vb5.result(); a6 <- vb6.result())
        yield (a1, a2, a3, a4, a5, a6)
    }

    joined
  }

  /** Perform an equijoin of six (6) distributed lists. */
  def join[K  : Manifest : WireFormat : Ordering,
           A1 : Manifest : WireFormat,
           A2 : Manifest : WireFormat,
           A3 : Manifest : WireFormat,
           A4 : Manifest : WireFormat,
           A5 : Manifest : WireFormat,
           A6 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)])
    : DList[(A1, A2, A3, A4, A5, A6)] = joinOn(d1, d2, d3, d4, d5, d6)(identity)


  /** Perform an inner-join of seven (7) distributed lists using a specified join-predicate. */
  def joinOn[K  : Manifest : WireFormat,
             T  : Manifest : WireFormat : Ordering,
             A1 : Manifest : WireFormat,
             A2 : Manifest : WireFormat,
             A3 : Manifest : WireFormat,
             A4 : Manifest : WireFormat,
             A5 : Manifest : WireFormat,
             A6 : Manifest : WireFormat,
             A7 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)], d7: DList[(K, A7)])
      (jp: K => T)
    : DList[(A1, A2, A3, A4, A5, A6, A7)] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d1 map { case (k, a1) => (jp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d2 map { case (k, a2) => (jp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d3 map { case (k, a3) => (jp(k), Some3(a3)) }
    val d4s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d4 map { case (k, a4) => (jp(k), Some4(a4)) }
    val d5s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d5 map { case (k, a5) => (jp(k), Some5(a5)) }
    val d6s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d6 map { case (k, a6) => (jp(k), Some6(a6)) }
    val d7s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d7 map { case (k, a7) => (jp(k), Some7(a7)) }

    val joined = (d1s ++ d2s ++ d3s ++ d4s ++ d5s ++ d6s ++ d7s).groupByKey flatMap { case (_, as) =>
      val vb1: VectorBuilder[A1] = new VectorBuilder[A1]()
      val vb2: VectorBuilder[A2] = new VectorBuilder[A2]()
      val vb3: VectorBuilder[A3] = new VectorBuilder[A3]()
      val vb4: VectorBuilder[A4] = new VectorBuilder[A4]()
      val vb5: VectorBuilder[A5] = new VectorBuilder[A5]()
      val vb6: VectorBuilder[A6] = new VectorBuilder[A6]()
      val vb7: VectorBuilder[A7] = new VectorBuilder[A7]()
      as foreach {
        case Some1(a1) => vb1 += a1
        case Some2(a2) => vb2 += a2
        case Some3(a3) => vb3 += a3
        case Some4(a4) => vb4 += a4
        case Some5(a5) => vb5 += a5
        case Some6(a6) => vb6 += a6
        case Some7(a7) => vb7 += a7
      }
      for (a1 <- vb1.result(); a2 <- vb2.result(); a3 <- vb3.result(); a4 <- vb4.result(); a5 <- vb5.result(); a6 <- vb6.result(); a7 <- vb7.result())
        yield (a1, a2, a3, a4, a5, a6, a7)
    }

    joined
  }

  /** Perform an equijoin of seven (7) distributed lists. */
  def join[K  : Manifest : WireFormat : Ordering,
           A1 : Manifest : WireFormat,
           A2 : Manifest : WireFormat,
           A3 : Manifest : WireFormat,
           A4 : Manifest : WireFormat,
           A5 : Manifest : WireFormat,
           A6 : Manifest : WireFormat,
           A7 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)], d7: DList[(K, A7)])
    : DList[(A1, A2, A3, A4, A5, A6, A7)] = joinOn(d1, d2, d3, d4, d5, d6, d7)(identity)


  /** Perform an inner-join of eight (8) distributed lists using a specified join-predicate. */
  def joinOn[K  : Manifest : WireFormat,
             T  : Manifest : WireFormat : Ordering,
             A1 : Manifest : WireFormat,
             A2 : Manifest : WireFormat,
             A3 : Manifest : WireFormat,
             A4 : Manifest : WireFormat,
             A5 : Manifest : WireFormat,
             A6 : Manifest : WireFormat,
             A7 : Manifest : WireFormat,
             A8 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)], d7: DList[(K, A7)], d8: DList[(K, A8)])
      (jp: K => T)
    : DList[(A1, A2, A3, A4, A5, A6, A7, A8)] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d1 map { case (k, a1) => (jp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d2 map { case (k, a2) => (jp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d3 map { case (k, a3) => (jp(k), Some3(a3)) }
    val d4s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d4 map { case (k, a4) => (jp(k), Some4(a4)) }
    val d5s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d5 map { case (k, a5) => (jp(k), Some5(a5)) }
    val d6s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d6 map { case (k, a6) => (jp(k), Some6(a6)) }
    val d7s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d7 map { case (k, a7) => (jp(k), Some7(a7)) }
    val d8s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d8 map { case (k, a8) => (jp(k), Some8(a8)) }

    val joined = (d1s ++ d2s ++ d3s ++ d4s ++ d5s ++ d6s ++ d7s ++ d8s).groupByKey flatMap { case (_, as) =>
      val vb1: VectorBuilder[A1] = new VectorBuilder[A1]()
      val vb2: VectorBuilder[A2] = new VectorBuilder[A2]()
      val vb3: VectorBuilder[A3] = new VectorBuilder[A3]()
      val vb4: VectorBuilder[A4] = new VectorBuilder[A4]()
      val vb5: VectorBuilder[A5] = new VectorBuilder[A5]()
      val vb6: VectorBuilder[A6] = new VectorBuilder[A6]()
      val vb7: VectorBuilder[A7] = new VectorBuilder[A7]()
      val vb8: VectorBuilder[A8] = new VectorBuilder[A8]()
      as foreach {
        case Some1(a1) => vb1 += a1
        case Some2(a2) => vb2 += a2
        case Some3(a3) => vb3 += a3
        case Some4(a4) => vb4 += a4
        case Some5(a5) => vb5 += a5
        case Some6(a6) => vb6 += a6
        case Some7(a7) => vb7 += a7
        case Some8(a8) => vb8 += a8
      }
      for (a1 <- vb1.result(); a2 <- vb2.result(); a3 <- vb3.result(); a4 <- vb4.result(); a5 <- vb5.result(); a6 <- vb6.result(); a7 <- vb7.result(); a8 <- vb8.result())
        yield (a1, a2, a3, a4, a5, a6, a7, a8)
    }

    joined
  }

  /** Perform an equijoin of eight (8) distributed lists. */
  def join[K  : Manifest : WireFormat : Ordering,
           A1 : Manifest : WireFormat,
           A2 : Manifest : WireFormat,
           A3 : Manifest : WireFormat,
           A4 : Manifest : WireFormat,
           A5 : Manifest : WireFormat,
           A6 : Manifest : WireFormat,
           A7 : Manifest : WireFormat,
           A8 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)], d7: DList[(K, A7)], d8: DList[(K, A8)])
    : DList[(A1, A2, A3, A4, A5, A6, A7, A8)] = joinOn(d1, d2, d3, d4, d5, d6, d7, d8)(identity)


    /** Perform a left outer-join of two (2) distributed lists using a specified join-predicate. */
    def leftJoinOn[K  : Manifest : WireFormat,
                   T  : Manifest : WireFormat : Ordering,
                   A1 : Manifest : WireFormat,
                   A2 : Manifest : WireFormat]
        (d1: DList[(K, A1)], d2: DList[(K, A2)])
        (jp: K => T)
        (default: (T, A1) => A2)
      : DList[(A1, A2)] = {

      val d1s: DList[(T, Multi[A1, A2, A1, A1, A1, A1, A1, A1])] = d1 map { case (k, a1) => (jp(k), Some1(a1)) }
      val d2s: DList[(T, Multi[A1, A2, A1, A1, A1, A1, A1, A1])] = d2 map { case (k, a2) => (jp(k), Some2(a2)) }

      val joined = (d1s ++ d2s).groupByKey flatMap { case (key, as) =>
        val vb1: VectorBuilder[A1] = new VectorBuilder[A1]()
        val vb2: VectorBuilder[A2] = new VectorBuilder[A2]()

        as foreach {
          case Some1(a1) => vb1 += a1
          case Some2(a2) => vb2 += a2
        }

        val a1s = vb1.result()
        val a2s = vb2.result()

        if (List(a1s, a2s) forall {_.size > 0 }) {
          for (a1 <- a1s; a2 <- a2s) yield ((a1, a2))
        } else if (a1s.size > 0 && a2s.size == 0) {
          for (a1 <- a1s) yield ((a1, default(key, a1)))
        } else {
          Nil
        }
      }

      joined
    }

    /** Perform a left outer-join of two (2) distributed lists. */
    def leftJoin[K  : Manifest : WireFormat : Ordering,
                 A1 : Manifest : WireFormat,
                 A2 : Manifest : WireFormat]
        (d1: DList[(K, A1)], d2: DList[(K, A2)])
        (default: (K, A1) => A2)
      : DList[(A1, A2)] = leftJoinOn(d1, d2)(identity)(default)


    /** Perform a right outer-join of two (2) distributed lists using a specified join-predicate. */
    def rightJoinOn[K  : Manifest : WireFormat,
                    T  : Manifest : WireFormat : Ordering,
                    A1 : Manifest : WireFormat,
                    A2 : Manifest : WireFormat]
        (d1: DList[(K, A1)], d2: DList[(K, A2)])
        (jp: K => T)
        (default: (T, A2) => A1)
      : DList[(A1, A2)] = {

      val d1s: DList[(T, Multi[A1, A2, A1, A1, A1, A1, A1, A1])] = d1 map { case (k, a1) => (jp(k), Some1(a1)) }
      val d2s: DList[(T, Multi[A1, A2, A1, A1, A1, A1, A1, A1])] = d2 map { case (k, a2) => (jp(k), Some2(a2)) }

      val joined = (d1s ++ d2s).groupByKey flatMap { case (key, as) =>
        val vb1: VectorBuilder[A1] = new VectorBuilder[A1]()
        val vb2: VectorBuilder[A2] = new VectorBuilder[A2]()

        as foreach {
          case Some1(a1) => vb1 += a1
          case Some2(a2) => vb2 += a2
        }

        val a1s = vb1.result()
        val a2s = vb2.result()

        if (List(a1s, a2s) forall {_.size > 0 }) {
          for (a1 <- a1s; a2 <- a2s) yield ((a1, a2))
        } else if (a1s.size == 0 && a2s.size > 0) {
          for (a2 <- a2s) yield ((default(key, a2), a2))
        } else {
          Nil
        }
      }

      joined
    }

    /** Perform a left outer-join of two (2) distributed lists. */
    def rightJoin[K  : Manifest : WireFormat : Ordering,
                  A1 : Manifest : WireFormat,
                  A2 : Manifest : WireFormat]
        (d1: DList[(K, A1)], d2: DList[(K, A2)])
        (default: (K, A2) => A1)
      : DList[(A1, A2)] = rightJoinOn(d1, d2)(identity)(default)

}

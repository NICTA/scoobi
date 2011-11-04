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
import com.nicta.scoobi.DList
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.WireFormat._
import com.nicta.scoobi.lib.Multi._


object CoGroup {

  /** Perform a co-group of two (2) distributed lists using a specified grouping-predicate. */
  def coGroupOn[K  : Manifest : WireFormat,
                T  : Manifest : WireFormat : Ordering,
                A1 : Manifest : WireFormat,
                A2 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)])
      (cgp: K => T)
    : DList[(Iterable[A1], Iterable[A2])] = {

    val d1s: DList[(T, Multi[A1, A2, A1, A1, A1, A1, A1, A1])] = d1 map { case (k, a1) => (cgp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A1, A1, A1, A1, A1, A1])] = d2 map { case (k, a2) => (cgp(k), Some2(a2)) }

    val grouped = (d1s ++ d2s).groupByKey map { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      (a1s, a2s)
    }

    grouped
  }

  /** Perform a co-group of two (2) distributed lists using an equality predicate. */
  def coGroup[K  : Manifest : WireFormat : Ordering,
              A1 : Manifest : WireFormat,
              A2 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)])
    : DList[(Iterable[A1], Iterable[A2])] = coGroupOn(d1, d2)(identity)


  /** Perform a co-group of three (3) distributed lists using a specified grouping-predicate. */
  def coGroupOn[K  : Manifest : WireFormat,
                T  : Manifest : WireFormat : Ordering,
                A1 : Manifest : WireFormat,
                A2 : Manifest : WireFormat,
                A3 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)])
      (cgp: K => T)
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3])] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A1, A1, A1, A1, A1])] = d1 map { case (k, a1) => (cgp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A1, A1, A1, A1, A1])] = d2 map { case (k, a2) => (cgp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A1, A1, A1, A1, A1])] = d3 map { case (k, a3) => (cgp(k), Some3(a3)) }

    val grouped = (d1s ++ d2s ++ d3s).groupByKey map { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      (a1s, a2s, a3s)
    }

    grouped
  }

  /** Perform a co-group of three (3) distributed lists using an equality predicate. */
  def coGroup[K  : Manifest : WireFormat : Ordering,
              A1 : Manifest : WireFormat,
              A2 : Manifest : WireFormat,
              A3 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)])
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3])] = coGroupOn(d1, d2, d3)(identity)


  /** Perform a co-group of four (4) distributed lists using a specified grouping-predicate. */
  def coGroupOn[K  : Manifest : WireFormat,
                T  : Manifest : WireFormat : Ordering,
                A1 : Manifest : WireFormat,
                A2 : Manifest : WireFormat,
                A3 : Manifest : WireFormat,
                A4 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)])
      (cgp: K => T)
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3], Iterable[A4])] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A4, A1, A1, A1, A1])] = d1 map { case (k, a1) => (cgp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A4, A1, A1, A1, A1])] = d2 map { case (k, a2) => (cgp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A4, A1, A1, A1, A1])] = d3 map { case (k, a3) => (cgp(k), Some3(a3)) }
    val d4s: DList[(T, Multi[A1, A2, A3, A4, A1, A1, A1, A1])] = d4 map { case (k, a4) => (cgp(k), Some4(a4)) }

    val grouped = (d1s ++ d2s ++ d3s ++ d4s).groupByKey map { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      val a4s = as collect { case Some4(a4) => a4 }
      (a1s, a2s, a3s, a4s)
    }

    grouped
  }

  /** Perform a co-group of four (4) distributed lists using an equality predicate. */
  def coGroup[K  : Manifest : WireFormat : Ordering,
              A1 : Manifest : WireFormat,
              A2 : Manifest : WireFormat,
              A3 : Manifest : WireFormat,
              A4 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)])
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3], Iterable[A4])] = coGroupOn(d1, d2, d3, d4)(identity)


  /** Perform a co-group of five (5) distributed lists using a specified grouping-predicate. */
  def coGroupOn[K  : Manifest : WireFormat,
                T  : Manifest : WireFormat : Ordering,
                A1 : Manifest : WireFormat,
                A2 : Manifest : WireFormat,
                A3 : Manifest : WireFormat,
                A4 : Manifest : WireFormat,
                A5 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)])
      (cgp: K => T)
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3], Iterable[A4], Iterable[A5])] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A4, A5, A1, A1, A1])] = d1 map { case (k, a1) => (cgp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A4, A5, A1, A1, A1])] = d2 map { case (k, a2) => (cgp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A4, A5, A1, A1, A1])] = d3 map { case (k, a3) => (cgp(k), Some3(a3)) }
    val d4s: DList[(T, Multi[A1, A2, A3, A4, A5, A1, A1, A1])] = d4 map { case (k, a4) => (cgp(k), Some4(a4)) }
    val d5s: DList[(T, Multi[A1, A2, A3, A4, A5, A1, A1, A1])] = d5 map { case (k, a5) => (cgp(k), Some5(a5)) }

    val grouped = (d1s ++ d2s ++ d3s ++ d4s ++ d5s).groupByKey map { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      val a4s = as collect { case Some4(a4) => a4 }
      val a5s = as collect { case Some5(a5) => a5 }
      (a1s, a2s, a3s, a4s, a5s)
    }

    grouped
  }

  /** Perform a co-group of five (5) distributed lists using an equality predicate. */
  def coGroup[K  : Manifest : WireFormat : Ordering,
              A1 : Manifest : WireFormat,
              A2 : Manifest : WireFormat,
              A3 : Manifest : WireFormat,
              A4 : Manifest : WireFormat,
              A5 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)])
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3], Iterable[A4], Iterable[A5])] = coGroupOn(d1, d2, d3, d4, d5)(identity)


  /** Perform a co-group of six (6) distributed lists using a specified grouping-predicate. */
  def coGroupOn[K  : Manifest : WireFormat,
                T  : Manifest : WireFormat : Ordering,
                A1 : Manifest : WireFormat,
                A2 : Manifest : WireFormat,
                A3 : Manifest : WireFormat,
                A4 : Manifest : WireFormat,
                A5 : Manifest : WireFormat,
                A6 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)])
      (cgp: K => T)
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3], Iterable[A4], Iterable[A5], Iterable[A6])] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d1 map { case (k, a1) => (cgp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d2 map { case (k, a2) => (cgp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d3 map { case (k, a3) => (cgp(k), Some3(a3)) }
    val d4s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d4 map { case (k, a4) => (cgp(k), Some4(a4)) }
    val d5s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d5 map { case (k, a5) => (cgp(k), Some5(a5)) }
    val d6s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A1, A1])] = d6 map { case (k, a6) => (cgp(k), Some6(a6)) }

    val grouped = (d1s ++ d2s ++ d3s ++ d4s ++ d5s ++ d6s).groupByKey map { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      val a4s = as collect { case Some4(a4) => a4 }
      val a5s = as collect { case Some5(a5) => a5 }
      val a6s = as collect { case Some6(a6) => a6 }
      (a1s, a2s, a3s, a4s, a5s, a6s)
    }

    grouped
  }

  /** Perform a co-group of six (6) distributed lists using an equality predicate. */
  def coGroup[K  : Manifest : WireFormat : Ordering,
              A1 : Manifest : WireFormat,
              A2 : Manifest : WireFormat,
              A3 : Manifest : WireFormat,
              A4 : Manifest : WireFormat,
              A5 : Manifest : WireFormat,
              A6 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)])
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3], Iterable[A4], Iterable[A5], Iterable[A6])] = coGroupOn(d1, d2, d3, d4, d5, d6)(identity)


  /** Perform a co-group of seven (7) distributed lists using a specified grouping-predicate. */
  def coGroupOn[K  : Manifest : WireFormat,
                T  : Manifest : WireFormat : Ordering,
                A1 : Manifest : WireFormat,
                A2 : Manifest : WireFormat,
                A3 : Manifest : WireFormat,
                A4 : Manifest : WireFormat,
                A5 : Manifest : WireFormat,
                A6 : Manifest : WireFormat,
                A7 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)], d7: DList[(K, A7)])
      (cgp: K => T)
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3], Iterable[A4], Iterable[A5], Iterable[A6], Iterable[A7])] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d1 map { case (k, a1) => (cgp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d2 map { case (k, a2) => (cgp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d3 map { case (k, a3) => (cgp(k), Some3(a3)) }
    val d4s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d4 map { case (k, a4) => (cgp(k), Some4(a4)) }
    val d5s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d5 map { case (k, a5) => (cgp(k), Some5(a5)) }
    val d6s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d6 map { case (k, a6) => (cgp(k), Some6(a6)) }
    val d7s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A1])] = d7 map { case (k, a7) => (cgp(k), Some7(a7)) }

    val grouped = (d1s ++ d2s ++ d3s ++ d4s ++ d5s ++ d6s ++ d7s).groupByKey map { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      val a4s = as collect { case Some4(a4) => a4 }
      val a5s = as collect { case Some5(a5) => a5 }
      val a6s = as collect { case Some6(a6) => a6 }
      val a7s = as collect { case Some7(a7) => a7 }
      (a1s, a2s, a3s, a4s, a5s, a6s, a7s)
    }

    grouped
  }

  /** Perform a co-group of seven (7) distributed lists using an equality predicate. */
  def coGroup[K  : Manifest : WireFormat : Ordering,
              A1 : Manifest : WireFormat,
              A2 : Manifest : WireFormat,
              A3 : Manifest : WireFormat,
              A4 : Manifest : WireFormat,
              A5 : Manifest : WireFormat,
              A6 : Manifest : WireFormat,
              A7 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)], d7: DList[(K, A7)])
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3], Iterable[A4], Iterable[A5], Iterable[A6], Iterable[A7])] = coGroupOn(d1, d2, d3, d4, d5, d6, d7)(identity)


  /** Perform a co-group of eight (8) distributed lists using a specified grouping-predicate. */
  def coGroupOn[K  : Manifest : WireFormat,
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
      (cgp: K => T)
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3], Iterable[A4], Iterable[A5], Iterable[A6], Iterable[A7], Iterable[A8])] = {

    val d1s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d1 map { case (k, a1) => (cgp(k), Some1(a1)) }
    val d2s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d2 map { case (k, a2) => (cgp(k), Some2(a2)) }
    val d3s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d3 map { case (k, a3) => (cgp(k), Some3(a3)) }
    val d4s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d4 map { case (k, a4) => (cgp(k), Some4(a4)) }
    val d5s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d5 map { case (k, a5) => (cgp(k), Some5(a5)) }
    val d6s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d6 map { case (k, a6) => (cgp(k), Some6(a6)) }
    val d7s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d7 map { case (k, a7) => (cgp(k), Some7(a7)) }
    val d8s: DList[(T, Multi[A1, A2, A3, A4, A5, A6, A7, A8])] = d8 map { case (k, a8) => (cgp(k), Some8(a8)) }

    val grouped = (d1s ++ d2s ++ d3s ++ d4s ++ d5s ++ d6s ++ d7s ++ d8s).groupByKey map { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      val a4s = as collect { case Some4(a4) => a4 }
      val a5s = as collect { case Some5(a5) => a5 }
      val a6s = as collect { case Some6(a6) => a6 }
      val a7s = as collect { case Some7(a7) => a7 }
      val a8s = as collect { case Some8(a8) => a8 }
      (a1s, a2s, a3s, a4s, a5s, a6s, a7s, a8s)
    }

    grouped
  }

  /** Perform a co-group of eight (8) distributed lists using an equality predicate. */
  def coGroup[K  : Manifest : WireFormat : Ordering,
              A1 : Manifest : WireFormat,
              A2 : Manifest : WireFormat,
              A3 : Manifest : WireFormat,
              A4 : Manifest : WireFormat,
              A5 : Manifest : WireFormat,
              A6 : Manifest : WireFormat,
              A7 : Manifest : WireFormat,
              A8 : Manifest : WireFormat]
      (d1: DList[(K, A1)], d2: DList[(K, A2)], d3: DList[(K, A3)], d4: DList[(K, A4)], d5: DList[(K, A5)], d6: DList[(K, A6)], d7: DList[(K, A7)], d8: DList[(K, A8)])
    : DList[(Iterable[A1], Iterable[A2], Iterable[A3], Iterable[A4], Iterable[A5], Iterable[A6], Iterable[A7], Iterable[A8])] = coGroupOn(d1, d2, d3, d4, d5, d6, d7, d8)(identity)
}

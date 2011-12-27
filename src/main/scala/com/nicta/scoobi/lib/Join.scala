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
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      if (List(a1s, a2s) forall {_.size > 0 }) {
        for (a1 <- a1s; a2 <- a2s) yield ((a1, a2))
      } else {
        Nil
      }
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
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      if (List(a1s, a2s, a3s) forall {_.size > 0 }) {
        for (a1 <- a1s; a2 <- a2s; a3 <- a3s) yield ((a1, a2, a3))
      } else {
        Nil
      }
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

    val joined = (d1s ++ d2s ++ d3s).groupByKey flatMap { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      val a4s = as collect { case Some4(a4) => a4 }
      if (List(a1s, a2s, a3s, a4s) forall {_.size > 0 }) {
        for (a1 <- a1s; a2 <- a2s; a3 <- a3s; a4 <- a4s) yield ((a1, a2, a3, a4))
      } else {
        Nil
      }
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

    val joined = (d1s ++ d2s ++ d3s).groupByKey flatMap { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      val a4s = as collect { case Some4(a4) => a4 }
      val a5s = as collect { case Some5(a5) => a5 }
      if (List(a1s, a2s, a3s, a4s, a5s) forall {_.size > 0 }) {
        for (a1 <- a1s; a2 <- a2s; a3 <- a3s; a4 <- a4s; a5 <- a5s) yield ((a1, a2, a3, a4, a5))
      } else {
        Nil
      }
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

    val joined = (d1s ++ d2s ++ d3s).groupByKey flatMap { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      val a4s = as collect { case Some4(a4) => a4 }
      val a5s = as collect { case Some5(a5) => a5 }
      val a6s = as collect { case Some6(a6) => a6 }
      if (List(a1s, a2s, a3s, a4s, a5s, a6s) forall {_.size > 0 }) {
        for (a1 <- a1s; a2 <- a2s; a3 <- a3s; a4 <- a4s; a5 <- a5s; a6 <- a6s) yield ((a1, a2, a3, a4, a5, a6))
      } else {
        Nil
      }
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

    val joined = (d1s ++ d2s ++ d3s).groupByKey flatMap { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      val a4s = as collect { case Some4(a4) => a4 }
      val a5s = as collect { case Some5(a5) => a5 }
      val a6s = as collect { case Some6(a6) => a6 }
      val a7s = as collect { case Some7(a7) => a7 }
      if (List(a1s, a2s, a3s, a4s, a5s, a6s, a7s) forall {_.size > 0 }) {
        for (a1 <- a1s; a2 <- a2s; a3 <- a3s; a4 <- a4s; a5 <- a5s; a6 <- a6s; a7 <- a7s) yield ((a1, a2, a3, a4, a5, a6, a7))
      } else {
        Nil
      }
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

    val joined = (d1s ++ d2s ++ d3s).groupByKey flatMap { case (_, as) =>
      val a1s = as collect { case Some1(a1) => a1 }
      val a2s = as collect { case Some2(a2) => a2 }
      val a3s = as collect { case Some3(a3) => a3 }
      val a4s = as collect { case Some4(a4) => a4 }
      val a5s = as collect { case Some5(a5) => a5 }
      val a6s = as collect { case Some6(a6) => a6 }
      val a7s = as collect { case Some7(a7) => a7 }
      val a8s = as collect { case Some8(a8) => a8 }
      if (List(a1s, a2s, a3s, a4s, a5s, a6s, a7s, a8s) forall {_.size > 0 }) {
        for (a1 <- a1s; a2 <- a2s; a3 <- a3s; a4 <- a4s; a5 <- a5s; a6 <- a6s; a7 <- a7s; a8 <- a8s) yield ((a1, a2, a3, a4, a5, a6, a7, a8))
      } else {
        Nil
      }
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
        val a1s = as collect { case Some1(a1) => a1 }
        val a2s = as collect { case Some2(a2) => a2 }

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
        val a1s = as collect { case Some1(a1) => a1 }
        val a2s = as collect { case Some2(a2) => a2 }

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

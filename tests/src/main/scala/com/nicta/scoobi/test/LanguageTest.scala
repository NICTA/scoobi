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
package com.nicta.scoobi.test

import java.io._
import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.lib.Join._


/** Test the language front-end */
object LanguageTest {

  import DList._
  import io.text.TextInput._
  import io.text.TextOutput._
  
  val names = DList((1, "Joe"), (2, "Kate"), (3, "Peter"), (4, "Rami"), (6, "Fred"))
  val cnts = DList((1,36.8), (2,92.3), (3,88.6), (6,12.9), (7,59.2), (2,49.1), (3,22.2))

  /** "Join" example. */
  def joinTest() = {

    val joined = join(names, cnts)
    val coGrouped = coGroup(names, cnts)

    persist(toTextFile(joined, "test-out/id-names-cnt"),
            toTextFile(coGrouped, "test-out/id-names-cnt-cg"))
  }

  def graphTest() = {
    import com.nicta.scoobi.impl.plan.Intermediate._

    val d1: DList[(Int, String)] = names
    val d2: DList[(Int, Iterable[String])] = d1.groupByKey
    val d2_ = d1.groupByKey

    val d3 = d2.flatMap(_._2)
    val d4: DList[(Int, String)] = d2_.combine(_++_)

    persist (
      toTextFile(d3, "test-out/d3"),
      toTextFile(d4, "test-out/d4")
    )
  }

  def noReducerTest() = {
    val d1: DList[(Int, Iterable[String])] = names.groupByKey

    persist(toTextFile(d1, "/out/d1"))
  }

  def bypassInputChannelTest() = {
    val d1: DList[(Int, Iterable[String])] = names.groupByKey
    val d2: DList[(Int, Iterable[Iterable[String]])] = d1.groupByKey

    persist(toTextFile(d2, "test-out/d2"))
  }

  def flatMapSiblingsTest() = {
    val d0: DList[(Int, String)] = names
    val d02: DList[(Int, String)] = names
    val d0_0: DList[(Int, String)] = d0.flatMap{case (i,str) => List((i, str + " for d0_0"))}
    val d0_1: DList[(Int, String)] = d0.flatMap{case (i,str) => List((i, str + " for d0_1"))}
    val d1: DList[(Int, String)] = d0_0 ++ d0_1
    val d2: DList[(Int, Iterable[String])] = d1.groupByKey
    val d02_0: DList[(Int, String)] = d02.flatMap{case (i,str) => List((i, str + "for d02_0"))}
    val d3: DList[(Int, Iterable[String])] = d02_0.groupByKey

    persist(toTextFile(d2, "test-out/fms-d2"),
            toTextFile(d3, "test-out/fms-d3"))
  }


  /** Test out fusion of flattens and flatMap. */
  def optimiseTest() = {
    import com.nicta.scoobi.impl.plan.Intermediate._

    val d1 = fromTextFile("whatever")
    val d2 = fromTextFile("whatever, again")
    val d3 = fromTextFile("whatever, again, and again")

    val f1 = d1 ++ d2 ++ d3

    val m1 = f1 map ((_, 42))
    val m2 = m1 map (identity)
    val m3 = m2 map (identity)

    val g1 = m1.groupByKey
    val g2 = m2.groupByKey
    val g3 = m3.groupByKey

    val c3 = g3.combine((a: Int, b: Int) => a + b)
    val r3 = c3.map(identity)

    val m4 = f1 map ((_, List(42, 43, 44).toIterable))
    val c2 = m4.combine((a: Int, b: Int) => a + b)
    val m5 = c2 map (identity)

    val c4 = g2.combine((a: Int, b: Int) => a + b)
    val m6 = g2.map(t => (t._1, t._2.tail.foldLeft(t._2.head)(_+_)))
  }


  /** Run them. */
  def main(args: Array[String]) {
    Scoobi.setJarByClass(this.getClass)
    println("-------------- join -------------"); joinTest()
    println("------------ graphTest ----------"); graphTest()
    println("----- bypassInputChannelTest ----"); bypassInputChannelTest()
    println("---------- flatMapSiblingsTest ---------"); flatMapSiblingsTest()
  }
}

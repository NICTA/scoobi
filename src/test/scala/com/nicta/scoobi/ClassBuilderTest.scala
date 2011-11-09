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
package com.nicta.scoobi

import java.io._
import DList._
import io.text.TextInput._
import io.text.TextOutput._


/* Make a simple cyclic data structure TODO: is there something better from the standard library? */

case class CircularLinkedList()
{
  case class Node(var next: Node, var payload: Int)

  var start: Node = null

  def cons(element: Int) {
    if (start == null) {
      start = Node(null, element)
      start.next = start
    } else {
      val oldStart = start
      start = Node(oldStart.next, element)
      oldStart.next = start
    }
  }

  def length(): Int = {
    var count = 0
    traverse( _ => count = count + 1)
    count
  }

  def sum(): Int = {
    var n = 0;
    traverse( (node: Node) => n = n + node.payload )
    n
  }

  private def traverse(f: Node =>  Unit) {
    if (start == null)
      0
    else {
      var it: Node = start

      do {
        f(it)
        it = it.next
      } while (it ne start)
    }
  }
}

object ClassBuilderTest {

  case class Dummy()

  implicit val DummyConversion = new WireFormat[Dummy] {

    val elements = scala.util.Random.nextInt(100) + 100
    var sum = 0;
    val holder = CircularLinkedList()

    (1 to elements).foreach { _ =>
      val n = scala.util.Random.nextInt(50)
      sum += n
      holder.cons(n)
    }

   if (elements < 100 || elements > 200 || holder.sum() != sum || holder.length != elements)
        sys.error("Something when terribly wrong")

    override def toWire(obj: Dummy, out: DataOutput) {
      if (elements < 100 || elements > 200 || holder.sum() != sum || holder.length != elements)
        sys.error("Something when terribly wrong")
      else
        println("Passed in toWire, count is: ", holder.length)
    }

    override def fromWire(in: DataInput): Dummy = {
      if (elements < 100 || elements > 200 || holder.sum() != sum || holder.length != elements)
        sys.error("Something when terribly wrong")
      else
        println("Passed in fromWire")

      Dummy()
    }

    override def show(x: Dummy): String = "Dummy"
  }


  def main(args: Array[String]) {
    val d1: DList[String] = fromTextFile("src/test/resources/ints.txt")
    val d2: DList[Dummy] = d1.map(_ => Dummy())

    persist(toTextFile(d2, "src/test/resources/out/dummy"))
  }
}

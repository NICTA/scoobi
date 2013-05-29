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
package core

import org.apache.hadoop.io.Text

import testing.mutable.NictaSimpleJobs
import Scoobi._
import org.specs2.matcher.TerminationMatchers
import org.specs2.ScalaCheck
import org.scalacheck.Prop
import impl.plan.comp.CompNodeData._
import org.scalacheck.Arbitrary._

class DListSpec extends NictaSimpleJobs with TerminationMatchers with ScalaCheck {

  "it must be possible to create an empty DList and persist it" >> { implicit sc: SC =>
    val list = fromTextFile(Seq("missing"), check = Source.noInputCheck)
    list.run === Vector()
  }

  tag("issue 99")
  "a DList can be created and persisted with some Text" >> { implicit sc: SC =>
    val list = DList(("key1", "value1"), ("key2", "value2")).map { case (k, v) => (new Text(k), new Text(v)) }
    run(list).map(_.toString).sorted must_== Seq("(key1,value1)", "(key2,value2)")
  }

  tag("issue 117")
  "A complex graph example must not throw an exception" >> { implicit sc: SC =>

    def simpleJoin[T: WireFormat, V: WireFormat](a: DList[(Int, T)], b: DList[(Int, V)]) =
      (a.map(x => (x._1, x._1)) ++ b.map(x => (x._1, x._1))).groupByKey

    val data = DList((12 -> 13), (14 -> 15), (13 -> 55))
    val (a, b, c, d, e) = (data, data, data, data, data)

    // joinab = joincd = Vector((12,Vector(12, 12)), (13,Vector(13, 13)), (14,Vector(14,14)))
    val (joinab, joincd) = (simpleJoin(a, b), simpleJoin(c, d))
    // q = Vector((12,Vector(12, 12)), (13,Vector(13, 13)), (14,Vector(14,14)))
    val q = simpleJoin(joinab, joincd)
    // qe = Vector((12,Vector(12, 12)), (13,Vector(13, 13)), (14,Vector(14,14)))
    val qe = simpleJoin(q, e).groupByKey
    val res = simpleJoin(q, qe)

    normalise(res.run) === "Vector((12,Vector(12, 12)), (13,Vector(13, 13)), (14,Vector(14, 14)))"
  }

  tag("issue 119")
  "joining an object created from random elements and a DList must not crash" >> { implicit sc: SC =>
    val r = new scala.util.Random

    val s = (1 to 10).map(i => (i, r.nextInt(i))).
                      groupBy(_._2).
                      mapValues(r.shuffle(_))

    (DObject(s) join DList(1, 2, 3)).run must not(throwAn[Exception])
  }

  tag("issue 137")
  "DList.concat will concatenate multiple DLists." >> { implicit sc: SC =>
    val aa = DList(1 to 5)
    val bb = DList(6 to 10)

    (aa ++ bb).run.sorted must_== (1 to 10).toSeq
  }

  tag("issue 194")
  "Length of an empty list should be zero" >> { implicit sc: SC =>
     DList[Int]().length.run === 0
  }

  "DLists can be concatenated via reduce" >> {
    "without group by key" >> { implicit sc: SC =>
      Seq.fill(5)(DList(1 -> 2)).reduce(_++_).run === Seq.fill(5)(1 -> 2)
    }
    "with a group by key" >> { implicit sc: SC =>
      Seq.fill(5)(DList(1 -> 2)).reduce(_++_).groupByKey.run.toList.toString === Seq(1 -> Vector.fill(5)(2)).toString
    }
  }

  "DList zipWithIndex works" >> { implicit sc: SC =>
    val len   = 1
    val dlist = DList(1 to len).map(_.toString)

    val withIndexes = dlist.zipWithIndex
    val words       = withIndexes.map(_._1)
    val indexes     = withIndexes.map(_._2)


    val (uniqueWords, minIndex, uniqueIndexes, maxIndex) = run(words.distinct.size, indexes.min, indexes.distinct.size, indexes.max)

    uniqueWords   === len
    minIndex      === 0
    uniqueIndexes === len
    maxIndex      === (len - 1)
  }
  
  "DList distinct works" >> { implicit sc: SC =>
    implicit def fmt = mkCaseWireFormat(PoorHashString, PoorHashString.unapply _)
    
    val words = (1 to 1000).map { x => PoorHashString(util.Random.nextString(1)) }
    
    words.toDList.distinct.size.run must_== words.distinct.size
  }

  "DList isEqual works" >> { implicit sc: SC =>
    
    val as = (1 to 100).map(scala.util.Random.nextInt(_).toString) // lots of dupes
    val bs = scala.util.Random.shuffle(as)
    
    val a = as.toDList
    val b = bs.toDList
    val a1 = a.filter(_ != bs.head)
    
    run((a isEqual a, a isEqual b, b isEqual a, a1 isEqual a)) must_== (true, true, true, false)
  }

  "A shuffled DList contains the same elements" >> { implicit sc: SC =>
    val shuffleProp = (list: List[Int]) => {
      val orig = list.toDList
      run(orig.shuffle isEqual orig) must_== true
    }
    Prop.forAll(shuffleProp).set(minTestsOk = 5, minSize = 0, maxSize = 100000)
  }

  tag("issue 256")
  "A DList can be created from a sequence of elements which will only be evaluated when executed" >> { implicit sc: SC =>
    val out: StringBuffer = new StringBuffer
    val list = fromLazySeq(Seq(1,
    {out.append("evaluating effect once".pp); 2},
    3))
    "effect is not evaluated" ==> { out.toString must beEmpty }
    list.run.normalise === "Vector(1, 2, 3)"
    "effect is evaluated" ==> { out.toString must not be empty }.unless(sc.isRemote)
  }
  
  "DList diff'ing actually works works" >> { implicit sc: SC =>
    
    val eqProp = (first: List[Int], second: List[Int]) => {
      run(first.toDList diff second.toDList).sorted must_== (first diff second).sorted
      run(first.toDList distinctDiff second.toDList).sorted must_== (first.toSet diff second.toSet).toList.sorted
    }
    
    Prop.forAllNoShrink(arbitrary[List[Int]], arbitrary[List[Int]])(eqProp).set(minTestsOk = 5, minSize = 0, maxSize = 10)
  }
  
  "DList partition works" >> { implicit sc: SC =>
    val shuffleProp = (list: List[Int]) => {
      val (l, r) = list.toDList.partition(_ => util.Random.nextBoolean)
      (run(l) ++ run(r)).sorted.toList must_== list.sorted
    }
    Prop.forAllNoShrink(arbitrary[List[Int]])(shuffleProp).set(minTestsOk = 5, minSize = 0, maxSize = 100)
  }
}

case class PoorHashString(s: String) {
  override def hashCode() = s.hashCode() & 0xff
}


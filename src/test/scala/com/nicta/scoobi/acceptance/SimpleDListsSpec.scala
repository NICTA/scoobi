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
package acceptance

import testing.mutable.NictaSimpleJobs
import com.nicta.scoobi.Scoobi._
import impl.plan.DListImpl
import com.nicta.scoobi.impl.plan.comp.factory._
import impl.plan.comp.{Return, ParallelDo, Load, CompNodeData}
import CompNodeData._
import io.ConstantStringDataSource
import core.WireFormat._
import core.{EmitterWriter, DoFunction, EmitterDoFunction}
import core.Reduction.{Reduction => R}

class SimpleDListsSpec extends NictaSimpleJobs with CompNodeData { section("unstable")

  "1. load" >> { implicit sc: SC =>
    DList("hello").run === Seq("hello")
  }
  "2. map" >> { implicit sc: SC =>
    DList("hello").map(_.size).run === Seq(5)
  }
  "3. groupByKey" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.run must be_==(Seq((1, Seq("hello", "world")))) or be_==(Seq((1, Seq("world", "hello"))))
  }
  "4. groupByKey + combine" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.combine(R.string).run must be_==(Seq((1, "helloworld"))) or be_==(Seq((1, "worldhello")))
  }
  "5. filter" >> { implicit sc: SC =>
    DList("hello", "world").filter(_.startsWith("h")).run === Seq("hello")
  }
  "6. flatMap" >> { implicit sc: SC =>
    DList("hello", "world").mapFlatten(_.toSeq.filterNot(_ == 'l')).run.toSet === Set('h', 'e', 'o', 'w', 'r', 'd')
  }
  "7. flatMap + map" >> { implicit sc: SC =>
    DList("hello", "world").mapFlatten(_.toSeq.filterNot(_ == 'l')).map(_.toUpper).run.toSet === Set('H', 'E', 'O', 'W', 'R', 'D')
  }
  "8. groupByKey + filter" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.filter { case (k, v) => k >= 1 }.run must
      be_==(Seq((1, Seq("hello", "world")))) or be_==(Seq((1, Seq("world", "hello"))))
  }
  "9. combine + filter" >> { implicit sc: SC =>
    DList((1, Seq("hello", "world")), (2, Seq("universe"))).combine(R.string).filter { case (k, v) => k >= 1 }.run.toSet must
      be_==(Set((1, "helloworld"), (2, "universe"))) or
      be_==(Set((1, "worldhello"), (2, "universe")))
  }
  "10. groupByKey + combine + groupByKey" >> { implicit sc: SC =>
    DList((1, "1")).filter(_ => true).groupByKey.combine(R.first).groupByKey.filter(_ => true).run === Seq((1, Seq("1")))
  }
  "11. groupByKey(flatten(groupByKey(l1), l1))" >> { implicit sc: SC =>
    val l0 = DList((1, "a"))
    val l1 = l0.groupByKey // (1, Iterable("a"))
    val l2 = l0.map { case (i, a) => (i, Iterable(a)) } // (1, Iterable("a"))
    val l = (l1 ++ l2).groupByKey // (1, Iterable(Iterable("a"), Iterable("a")))
    l.run must haveTheSameElementsAs(Seq((1, Iterable(Seq("a"), Seq("a")))))
  }
  "12. 2 parallelDos on a flatten" in { implicit sc: SC =>
    (DList("hello") ++ DList("world")).materialise.run.toSet === Set("hello", "world")
  }
  "13. join + pd + gbk + pd" in { implicit sc: SC =>
    val list = (DList("a").materialise.map(_.mkString) join DList("b")).groupByKey.map { case (k, v) => (k, v) }

    normalise(list.materialise.run) === "Vector((a,Vector(b)))"
  }
  "14. gbk1 with reducer + gbk2 with output feeding gbk1" >> { implicit sc: SC =>
    val l0 = DList((1, "a"))
    val l1 = l0.groupByKey.filter(_ => true)
    val l2 = l0.groupByKey.map { case (i, as) => "b" }.materialise
    val l3 = (l2 join l1).filter(_ => true)

    normalise(l1.run) === "Vector((1,Vector(a)))"
    normalise(l3.run) === "Vector((Vector(b),(1,Vector(a))))"
  }
  "15. 2 flattened parallelDos + gbk + reducer" >> { implicit sc: SC =>
    val (l1, l2) = (DList((1, "hello")), DList((1, "world")))
    val l3 = (l1 ++ l2).groupByKey.filter(_ => true).filter(_ => true)
    normalise(l3.run) === "Vector((1,Vector(hello, world)))"
  }
  "16. joinFullOuter" >> { implicit sc: SC =>
    val words = DList("apple", "orchard", "banana", "cat", "dancer")
    val letters = DList('a' -> 1, 'o' -> 4)

    val grouped = words.groupBy(word => word.head)
    normalise(letters.joinFullOuter(grouped).run) ===
      "Vector((a,(Some(1),Some(Vector(apple)))), (b,(None,Some(Vector(banana)))), (c,(None,Some(Vector(cat)))), (d,(None,Some(Vector(dancer)))), (o,(Some(4),Some(Vector(orchard)))))"
  }
  "17. parallelDo + gbk + combine + parallelDo + gbk + reducer" >> { implicit sc: SC =>
    val l1 = DList((1, "hello")).groupByKey.combine(R.string).map(_ => (1, "hello")).groupByKey.filter(_ => true)
    normalise(l1.run) === "Vector((1,Vector(hello)))"
  }
  "18. tree of parallelDos" >> { implicit sc: SC =>
    def list = DList("hello").map(_.partition(_ > 'm'))
    val (l1, l2, l4, l5) = (list, list, list, list)
    val (l3, l6) = (l1 ++ l2, l4 ++ l5)
    normalise((l3 ++ l6).run) === "Vector((o,hell), (o,hell), (o,hell), (o,hell))"
  }
  "19. join + gbk" >> { implicit sc: SC =>
    def list = DList("hello").map(_.partition(_ > 'm'))
    val l1 = list.groupByKey.map { case (k, vs) => k }. materialise
    val l2 = l1.join(DList("hello")).map { case (vs, k) => k }
    normalise(l2.run) must not(throwAn[Exception])
  }
  "20. nested parallelDos" >> { implicit sc: SC =>
    def list1 = DList("start").map(_.partition(_ > 'a')).map(_.toString)
    normalise(list1.run) === "Vector((strt,a))"
  }
  "21. more nested parallelDos" >> { implicit sc: SC =>
    def list1 = DList("start").map(_.partition(_ > 'a'))
    val (l1, l2) = (list1, list1)
    normalise((l1 ++ l2).filter(_ => true).run) === "Vector((strt,a), (strt,a))"
  }
  "22. parallelDo + combine" >> { implicit sc: SC =>
    val (l1, l2) = (DList("start").map(_.partition(_ > 'a')), DList("start").map(_.partition(_ > 'a')))
    val l3 = l1.map { case (k, v) => (k, Seq.fill(2)(v)) }.combine(R.string)
    val l4 = (l2 ++ l3).map(_.toString)
    normalise(l4.run) === "Vector((strt,a), (strt,aa))"
  }
  "23. (pd + pd) + gbk + reducer" >> { implicit sc: SC =>
    def list = (DList((1, "start")) ++ DList((1, "start")))
    val l3 = list.groupByKey.filter(_ => true).filter(_ => true)
    normalise(l3.run) === "Vector((1,Vector(start, start)))"
  }
  "24. join on a gbk" >> { implicit sc: SC =>
    val l1 = DList("hello").materialise
    val l2 = l1 join DList("a" -> "b").groupByKey.map(identity)// { case (a, bs) => (a, Vector(bs:_*)).toString }
    normalise(l2.run) === "Vector((Vector(hello),(a,Vector(b))))"
  }
  "25. flatMap" >> { implicit sc: SC =>
    normalise(DList("hello", "world").mapFlatten { w => Seq.fill(2)(w) }.run) ===
    "Vector(hello, hello, world, world)"
  }
  "26. (l1 ++ l2).groupByKey === (l1.groupByKey ++ l2.groupByKey).map { case (k, vs) => (k, vs.flatten) }" >> { implicit sc: SC =>
    val (l1, l2) = (DList(1 -> "hello", 2 -> "world"), DList(1 -> "hi", 2 -> "you"))
    normalise((l1 ++ l2).groupByKey.run) === normalise((l1.groupByKey ++ l2.groupByKey).groupByKey.map { case (k, vs) => (k, vs.flatten) }.run)
  }
  "27. a DList can be used in a for-comprehension" >> { implicit sc: SC =>
    val list = for {
      e  <- DList(1, 2, 3, 4) if e % 2 == 0
    } yield e

    normalise(list.run) === "Vector(2, 4)"
  }
  "28. pd + gbk + distinct+size + join" >> { implicit sc: SC =>
    val l0 = DList(1, 2, 1, 2)
    val l1 = l0.map(_ + 1)
    val l2 = l1.map(x => (x % 2, x))
    val l3 = l2.groupByKey.combine(R.Sum.int)
    val c = l1.distinct.size
    val l4 = c join l3
    normalise(l4.run) === "Vector((2,(0,4)), (2,(1,6)))"
  }
  "29. diamond of gbks" >> { implicit sc: SC =>
    val l1 = DList(1, 2)
    val (pd1, pd2)   = (l1.map(i => (i, i)), l1.map(i => (i, i)))
    val (gbk1, gbk2) = (pd1.groupByKey, pd2.groupByKey)
    val (pd3, pd4)   = (gbk1.map(identity), gbk2.map(identity))
    val pd5          = pd3 ++ pd4
    val gbk3         = pd5.groupByKey

    gbk3.run.normalise === "Vector((1,Vector(Vector(1), Vector(1))), (2,Vector(Vector(2), Vector(2))))"
  }
  "30. early exit of a mapper" >> { implicit sc: SC =>
    val l1 = DList(1, 2)
    val l2 = l1.map(i => (i, i))
    val l3 = l2.map(identity)
    val gbk = l2.groupByKey.combine(R.Sum.int)
    val result = l2 ++ gbk

    result.run.toSet === Set((1,1), (2,2), (1,1), (2,2))
  }
  "xxx" >> { implicit sc: SC =>
    val psv = DList(1, 2).map(identity)
    val json = DList[Int]().map(identity)
    val all = psv ++ json
    all.map { i => i.pp; i } .run must_== Seq(1, 2)
  }
}

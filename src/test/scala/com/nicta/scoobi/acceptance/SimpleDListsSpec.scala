package com.nicta.scoobi
package acceptance

import testing.mutable.NictaSimpleJobs
import com.nicta.scoobi.Scoobi._
import impl.plan.DListImpl
import com.nicta.scoobi.impl.plan.comp.factory._
import impl.plan.comp.CompNodeData

class SimpleDListsSpec extends NictaSimpleJobs with CompNodeData {

  "1. load" >> { implicit sc: SC =>
    DList("hello").run === Seq("hello")
  }

  "2. map" >> { implicit sc: SC =>
    DList("hello").map(_.size).run === Seq(5)
  }

  "3. groupByKey" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.run must haveTheSameElementsAs(Seq((1, Seq("hello", "world"))))
  }

  "4. groupByKey + combine" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.combine((_:String)+(_:String)).run must be_==(Seq((1, "helloworld"))) or be_==(Seq((1, "worldhello")))
  }

  "5. filter" >> { implicit sc: SC =>
    DList("hello", "world").filter(_.startsWith("h")).run === Seq("hello")
  }

  "6. flatMap" >> { implicit sc: SC =>
    DList("hello", "world").flatMap(_.toSeq.filterNot(_ == 'l')).run.toSet === Set('h', 'e', 'o', 'w', 'r', 'd')
  }

  "7. flatMap + map" >> { implicit sc: SC =>
    DList("hello", "world").flatMap(_.toSeq.filterNot(_ == 'l')).map(_.toUpper).run.toSet === Set('H', 'E', 'O', 'W', 'R', 'D')
  }

  "8. groupByKey + filter" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.filter { case (k, v) => k >= 1 }.run must
      haveTheSameElementsAs(Seq((1, Seq("hello", "world"))))
  }

  "9. combine + filter + groupBarrier" >> { implicit sc: SC =>
    DList((1, Seq("hello", "world")), (2, Seq("universe"))).combine((_:String)+(_:String)).filter { case (k, v) => k >= 1 }.groupBarrier.run.toSet must
      be_==(Set((1, "helloworld"), (2, "universe"))) or
      be_==(Set((1, "worldhello"), (2, "universe")))
  }
  "10. groupByKey + combine + groupByKey" >> { implicit sc: SC =>
    DList((1, "1")).filter(_ => true).groupByKey.combine((a: String, b: String) => a).groupByKey.filter(_ => true).run === Seq((1, Seq("1")))
  }
  "11. groupByKey(flatten(groupByKey(l1), l1))" >> { implicit sc: SC =>
    val l0 = DList((1, "a"))
    val l1 = l0.groupByKey // (1, Iterable("a"))
    val l2 = l0.map { case (i, a) => (i, Iterable(a)) } // (1, Iterable("a"))
    val l = (l1 ++ l2).groupByKey // (1, Iterable(Iterable("a"), Iterable("a")))
    l.run must haveTheSameElementsAs(Seq((1, Iterable(Seq("a"), Seq("a")))))
  }
  "12. 2 parallelDos on a flatten" in { implicit sc: SC =>
    (DList("hello") ++ DList("world")).materialize.run.toSet === Set("hello", "world")
  }
  "13. join + pd + gbk + pd" in { implicit sc: SC =>
    val list = (DList("a").materialize.map(_.mkString) join DList("b")).groupByKey.map { case (k, v) => (k, v) }

    normalize(list.materialize.run) === "Vector((a,Vector(b)))"
  }
  "14. gbk1 with reducer + gbk2 with output feeding gbk1" >> { implicit sc: SC =>
    val l0 = DList((1, "a"))
    val l1 = l0.groupByKey.filter(_ => true)
    val l2 = l0.groupByKey.map { case (i, as) => "b" }.materialize
    val l3 = (l2 join l1).filter(_ => true)

    normalize(l1.run) === "Vector((1,Vector(a)))"
    normalize(l3.run) === "Vector((Vector(b),(1,Vector(a))))"
  }
  "15. " >> { implicit sc: SC =>
    val words = DList("apple", "orchard", "banana", "cat", "dancer")
    val letters = DList('a' -> 1, 'o' -> 4)

    val grouped = words.groupBy(word => word.headOption.getOrElse('X'))
    letters.joinFullOuter(grouped).run must not(throwAn[Exception])
  }

}

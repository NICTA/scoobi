package com.nicta.scoobi
package acceptance

import testing.NictaSimpleJobs
import com.nicta.scoobi.Scoobi._
import impl.plan.DListImpl
import com.nicta.scoobi.impl.plan.comp.factory._

class SimpleDListsSpec extends NictaSimpleJobs {

  // for now this spec needs to be sequential otherwise example 8 doesn't pass
  sequential

  "1. load" >> { implicit sc: SC =>
    DList("hello").run === Seq("hello")
  }

  "2. map" >> { implicit sc: SC =>
    DList("hello").map(_.size).run === Seq(5)
  }

  "3. groupByKey" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.run must be_==(Seq((1, Seq("hello", "world")))) or
                                                          be_==(Seq((1, Seq("world", "hello"))))
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
      be_==(Seq((1, Seq("hello", "world")))) or be_==(Seq((1, Seq("world", "hello"))))
  }

  "9. combine + filter + groupBarrier" >> { implicit sc: SC =>
    DList((1, Seq("hello", "world")), (2, Seq("universe"))).combine((_:String)+(_:String)).filter { case (k, v) => k >= 1 }.groupBarrier.run.toSet must
      be_==(Set((1, "helloworld"), (2, "universe"))) or
      be_==(Set((1, "worldhello"), (2, "universe")))
  }
  "10. test" >> { implicit sc: SC =>
    new DListImpl(pd(gbk(cb(gbk(pd(load)))))).run
    ok
  }
}

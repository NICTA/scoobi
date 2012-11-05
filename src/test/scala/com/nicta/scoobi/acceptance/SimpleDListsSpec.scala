package com.nicta.scoobi
package acceptance

import testing.NictaSimpleJobs
import com.nicta.scoobi.Scoobi._

class SimpleDListsSpec extends NictaSimpleJobs {

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
    DList((1, "hello"), (1, "world")).groupByKey.combine((_:String)+(_:String)).run must be_==(Seq((1, "helloworld"))) or be_==(Seq((1, "worldhello")))
  }

  "5. filter" >> { implicit sc: SC =>
    DList("hello", "world").filter(_.startsWith("h")).run === Seq("hello")
  }

  "6. flatMap" >> { implicit sc: SC =>
    DList("hello", "world").flatMap(_.toSeq.filterNot(_ == 'l')).run.toSet === Set('h', 'e', 'w', 'r', 'd')
  }

  "7. flatMap + map" >> { implicit sc: SC =>
    DList("hello", "world").flatMap(_.toSeq.filterNot(_ == 'l')).map(_.toUpper).run.toSet === Set('H', 'E', 'O', 'W', 'R', 'D')
  }

  "8. groupByKey + filter" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.filter { case (k, v) => k >= 1 }.run must
      be_==(Seq((1, Seq("hello", "world")))) or be_==(Seq((1, Seq("world", "hello"))))
  }

}

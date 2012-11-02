package com.nicta.scoobi
package acceptance

import testing.NictaSimpleJobs
import com.nicta.scoobi.Scoobi._

class SimpleDListsSpec extends NictaSimpleJobs {
  override def context   = local
  override def keepFiles = true
  override def quiet     = false
  override def level     = application.level("ALL")

  args.select(ex = "combine")

  "load" >> { implicit sc: SC =>
    DList("hello").run === Seq("hello")
  }

  "load + map" >> { implicit sc: SC =>
    DList("hello").map(_.size).run === Seq(5)
  }

  "load + map + groupByKey" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.run === Seq((1, Seq("hello", "world")))
  }

  "load + map + groupByKey + combine" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.combine((_:String)+(_:String)).run must be_==(Seq((1, "helloworld"))) or be_==(Seq((1, "worldhello")))
  }
}

package com.nicta.scoobi
package acceptance

import testing.NictaSimpleJobs
import com.nicta.scoobi.Scoobi._

class SimpleDListsSpec extends NictaSimpleJobs {
  override def keepFiles = true
  override def context = cluster
  override def quiet = false
  override def level = application.level("ALL")

  "simple" >> { implicit sc: SC =>
    DList("hello").run === Seq("hello")
  }

}

package com.nicta.scoobi
package core

import testing.NictaHadoop
import Scoobi._

class DObjectSpec extends NictaHadoop {

  tag("issue 113")
  "it must be possible to take the minimum and the maximum of a list" >> { implicit sc: SC =>
    val r = DList(1, 2, 3, 4)
    persist(r.min, r.max) === (1, 4)
  }

}

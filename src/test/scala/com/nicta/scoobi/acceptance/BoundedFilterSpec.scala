package com.nicta.scoobi.acceptance

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.NictaHadoop

class BoundedFilterSpec extends NictaHadoop {

  "Filtering with lower and upper bounds removes all values outside a range" >> { c: SC =>

    val xs = DList(1, 2, 3, 4)

    val lower = DObject(1)
    val upper = DObject(4)

    val ys = ((lower, upper) join xs) filter {case ((l, u), x) => x > l && x < u}
    val total = ys.values.sum

    persist(c)(total) must_== 5

  }
}

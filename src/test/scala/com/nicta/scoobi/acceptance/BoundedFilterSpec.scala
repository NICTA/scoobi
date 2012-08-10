package com.nicta.scoobi
package acceptance

import Scoobi._
import testing.{NictaSimpleJobs, NictaHadoop}

class BoundedFilterSpec extends NictaSimpleJobs {

  "Filtering with DObjects" >> {
    "Filtering with lower and upper bounds removes all values outside a range" >> { implicit c: SC =>

      val xs = DList(1, 2, 3, 4)

      val lower = DObject(1)
      val upper = DObject(4)

      val ys = ((lower, upper) join xs) filter {case ((l, u), x) => x > l && x < u}
      val total = ys.values.sum

      total.run must_== 5

    }

    "Filtering by average removes all values less than the average" >> { implicit c: SC =>

      val ints = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

      val xs = ints.toDList
      val average = (xs.sum, xs.size) map { case (t, s) => t / s }
      val bigger = (average join xs) filter { case (a, x) => x > a }

      bigger.values.run.sorted must_== ints.filter(_ > (ints.sum / ints.size))

    }
  }
}

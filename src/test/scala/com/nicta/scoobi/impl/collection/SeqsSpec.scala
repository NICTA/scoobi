package com.nicta.scoobi
package impl
package collection

import org.specs2.mutable.Specification
import Seqs._

class SeqsSpec extends Specification {

  "A sequence can be splitted into several smaller ones" >> {
    split(Seq(), 3, Splitted)           === Seq()
    split(Seq(1), 3, Splitted)          === Seq(Splitted(0, 1, Seq(1)))
    split(Seq(1, 2), 3, Splitted)       === Seq(Splitted(0, 2, Seq(1, 2)))
    split(Seq(1, 2, 3), 3, Splitted)    === Seq(Splitted(0, 3, Seq(1, 2, 3)))
    split(Seq(1, 2, 3, 4), 3, Splitted) === Seq(Splitted(0, 3, Seq(1, 2, 3, 4)), Splitted(3, 1, Seq(1, 2, 3, 4)))
  }

  case class Splitted(offset: Int, length: Int, seq: Seq[Int])
}


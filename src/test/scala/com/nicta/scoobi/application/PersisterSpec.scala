package com.nicta.scoobi
package application

import Scoobi._
import testing.{NictaHadoop, TempFiles}
import testing.mutable.SimpleJobs
import testing.TestFiles._

class PersisterSpec extends NictaHadoop with SimpleJobs {

  "A sequence of DLists can be persisted simultaneously to text files" >> { implicit sc: ScoobiConfiguration =>
    val dirs = Seq.fill(3)(TempFiles.createTempDir("test"))
    val lists = Seq(DList(1, 2), DList(3, 4), DList(5, 6)).zip(dirs).map { case (l, d) => l.toTextFile(path(d)) }
    lists.map(_.persist)
    dirs.map(dirResults).flatten.toSet must_== Set("1","2","3","4","5","6")
  }
  "A tuple containing a sequence of DLists can be persisted to text files" >> { implicit sc: ScoobiConfiguration =>
    val dirs = Seq.fill(4)(TempFiles.createTempDir("test"))
    val lists = Seq(DList(1, 2), DList(3, 4), DList(5, 6)).zip(dirs).map { case (l, d) => l.toTextFile(path(d)) }
    val firstList = DList(7, 8, 9).toTextFile(path(dirs.last))
    (firstList +: lists).map(_.persist)
    dirs.map(dirResults).flatten.toSet must_== Set("1","2","3","4","5","6","7","8","9")
  }
  "A sequence of DLists can be persisted simultaneously with the run method" >> { implicit sc: ScoobiConfiguration =>
    val lists = Seq(DList(1, 2), DList(3, 4), DList(5, 6))
    lists.map(_.run).flatten.toSet must_== Set(1,2,3,4,5,6)
  }

}

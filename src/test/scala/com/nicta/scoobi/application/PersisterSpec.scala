/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package application

import Scoobi._
import testing.{NictaHadoop, TempFiles}
import testing.mutable.SimpleJobs
import testing.TestFiles._

class PersisterSpec extends NictaHadoop with SimpleJobs {
  "A sequence of DLists can be persisted simultaneously to text files" >> { implicit sc: ScoobiConfiguration =>
    val dirs = Seq.fill(3)(TempFiles.createTempDir("test"))
    val lists = Seq(DList(1, 2), DList(3, 4), DList(5, 6)).zip(dirs).map { case (l, d) => toTextFile(l, path(d)) }
    persist(lists)
    dirs.map(dirResults).flatten.toSet must_== Set("1","2","3","4","5","6")
  }
  "A tuple containing a sequence of DLists can be persisted to text files" >> { implicit sc: ScoobiConfiguration =>
    val dirs = Seq.fill(4)(TempFiles.createTempDir("test"))
    val lists = Seq(DList(1, 2), DList(3, 4), DList(5, 6)).zip(dirs).map { case (l, d) => toTextFile(l, path(d)) }
    val firstList = toTextFile(DList(7, 8, 9), path(dirs.last))
    persist((firstList, lists))
    dirs.map(dirResults).flatten.toSet must_== Set("1","2","3","4","5","6","7","8","9")
  }
  "A sequence of DLists can be persisted simultaneously with the run method" >> { implicit sc: ScoobiConfiguration =>
    val lists = Seq(DList(1, 2), DList(3, 4), DList(5, 6))
    lists.run.flatten.toSet must_== Set(1,2,3,4,5,6)
  }

}

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
package impl
package collection

import Seqs._
import org.specs2._
import scalaz.Zipper
import specification.Grouped
import Zipper._
import testing.UnitSpecification

class SeqsSpec extends UnitSpecification with ScalaCheck with Grouped { def is = s2"""

 A sequence can be splitted into several smaller ones
 ${ split(Seq(), 3, Splitted)           === Seq()                                                                 }
 ${ split(Seq(1), 3, Splitted)          === Seq(Splitted(0, 1, Seq(1)))                                           }
 ${ split(Seq(1, 2), 3, Splitted)       === Seq(Splitted(0, 2, Seq(1, 2)))                                        }
 ${ split(Seq(1, 2, 3), 3, Splitted)    === Seq(Splitted(0, 3, Seq(1, 2, 3)))                                     }
 ${ split(Seq(1, 2, 3, 4), 3, Splitted) === Seq(Splitted(0, 3, Seq(1, 2, 3, 4)), Splitted(3, 1, Seq(1, 2, 3, 4))) }

 A sequence can be partitioned into sub-sequences
   for each element in a subgroup, there exists another element satisfying a predicate   ${g1.e1}
   and there doesn't exist an element in any other subsequence satisfying the predicate  ${g1.e2}
   2 elements which not satifying the predicate must end up in different groups          ${g1.e3}
                                                                                         """

  "groupWhen" - new g1 {
    e1 := prop { (list: List[Int]) =>
      val groups = groupWhen((0 :: list))(predicate)
      groups must contain(similarElements).forall
    }
    e2 := prop { (list: List[Int]) =>
      val groups = groupWhen(0 :: list)(predicate).toStream
      (zipper(Stream.empty, groups.head, groups.drop(1)).positions.toStream must not contain(sharedElements))
    }
    e3 := prop { (numbers: List[Int]) =>
      val distinct = (0 :: numbers).distinct
      val groups = groupWhen(distinct)(alwaysFalse)
      groups must have size(distinct.size)
    }
  }

  val predicate   = (n1: Int, n2: Int) => (n1 - n2) % 3 == 0
  val alwaysFalse = (n1: Int, n2: Int) => false

  /** check if all elements of the sequence are "connected" with each other through the predicate */
  def similarElements = (seq: Seq[Int]) => {
    seq must contain((e1: Int) => seq must contain((e2: Int) => predicate(e1, e2))).atLeastOnce
  }
  /** check that one group (the zipper's focus) doesn't share any element with the other groups */
  def sharedElements = (groups: Zipper[Seq[Int]]) =>
    groups.focus must not contain { e1: Int =>
      (groups.lefts ++ groups.rights) must not contain((group: Seq[Int]) => group.contains((e2: Int) => predicate(e1, e2)))
    }


  case class Splitted(offset: Int, length: Int, seq: Seq[Int])
}


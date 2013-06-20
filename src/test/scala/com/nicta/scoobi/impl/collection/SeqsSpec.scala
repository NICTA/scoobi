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
import scalaz.{NonEmptyList, Zipper}
import Zipper._
import NonEmptyList._
import scalaz.syntax.cojoin._
import specification.Grouped
import testing.UnitSpecification

class SeqsSpec extends UnitSpecification with ScalaCheck with Grouped { def is = s2"""

 A sequence can be splitted into several smaller ones
 ${ split(Seq(), 3, Splitted)           === Seq()                                                                 }
 ${ split(Seq(1), 3, Splitted)          === Seq(Splitted(0, 1, Seq(1)))                                           }
 ${ split(Seq(1, 2), 3, Splitted)       === Seq(Splitted(0, 2, Seq(1, 2)))                                        }
 ${ split(Seq(1, 2, 3), 3, Splitted)    === Seq(Splitted(0, 3, Seq(1, 2, 3)))                                     }
 ${ split(Seq(1, 2, 3, 4), 3, Splitted) === Seq(Splitted(0, 3, Seq(1, 2, 3, 4)), Splitted(3, 1, Seq(1, 2, 3, 4))) }

 A sequence can be partitioned into sub-sequences
   for each element in a group, there exists at least another related element in the group  ${g1.e1}
   for each element in a group, there doesn't exist a related element in any other group    ${g1.e2}
   2 elements which are not related must end up in different groups                         ${g1.e3}
                                                                                            """

  "partition" - new g1 {
    e1 := prop { (list: List[Int], relation: (Int, Int) => Boolean) =>
      val groups = partition(list)(relation)
      groups must contain(relatedElements(relation)).forall
    }

    e2 := prop { (list: List[Int], relation: (Int, Int) => Boolean) =>
      val groups = partition(list)(relation)
      groups match {
        case Nil          => list must beEmpty
        case head :: tail =>
          nel(head, tail).toZipper.cojoin.toStream must not contain(relatedElementsAcrossGroups(relation))
      }
    }

    e3 := prop { (list: List[Int]) =>
      val neverRelated = (n1: Int, n2: Int) => false
      val groups = partition(list)(neverRelated)
      groups must have size(list.size)
    }
  }

  def relatedElements(relation: (Int, Int) => Boolean) = (group: NonEmptyList[Int]) => {
    group.toZipper.cojoin.toStream must contain { zipper: Zipper[Int] =>
      (zipper.lefts ++ zipper.rights) must contain(relation.curried(zipper.focus)).forall
    }
  }

  def relatedElementsAcrossGroups(relation: (Int, Int) => Boolean) =
    (groups: Zipper[NonEmptyList[Int]]) =>
      groups.focus.list must not contain { e1: Int =>
        (groups.lefts ++ groups.rights) must not contain(relatedElements(relation))
      }

  case class Splitted(offset: Int, length: Int, seq: Seq[Int])
}


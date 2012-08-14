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
package acceptance

import Scoobi._
import testing.NictaSimpleJobs
import application.ScoobiConfiguration

import SecondarySort._

class SecondarySortSpec extends NictaSimpleJobs {
  "We can do a secondary sort by using a Grouping on the key" >> { implicit sc: ScoobiConfiguration =>

    val names: DList[(FirstName, LastName)] = DList(
      ("Michael", "Jackson"),
      ("Leonardo", "Da Vinci"),
      ("John", "Kennedy"),
      ("Mark", "Twain"),
      ("Bat", "Man"),
      ("Michael", "Jordan"),
      ("Mark", "Edison"),
      ("Michael", "Landon"),
      ("Leonardo", "De Capro"),
      ("Michael", "J. Fox"))

    val bigKey: DList[((FirstName, LastName), LastName)] = names.map(a => ((a._1, a._2), a._2))

    bigKey.groupByKey.map { case ((first, last), values) => ((first, last), values.mkString(", ")) }.run.mkString === Seq(
      "((Bat,Man),Man)",
      "((John,Kennedy),Kennedy)",
      "((Leonardo,Da Vinci),Da Vinci, De Capro)",
      "((Mark,Edison),Edison, Twain)",
      "((Michael,J. Fox),J. Fox, Jackson, Jordan, Landon)").mkString
  }
}

object SecondarySort {

  type FirstName = String
  type LastName = String

  implicit val grouping: Grouping[(FirstName, LastName)] = new Grouping[(FirstName, LastName)] {

    override def partition(key: (FirstName, LastName), howManyReducers: Int): Int =
      implicitly[Grouping[FirstName]].partition(key._1, howManyReducers)

    override def sortCompare(a: (FirstName, LastName), b: (FirstName, LastName)): Int = {
      val firstNameOrdering = groupCompare(a, b)
      firstNameOrdering match  {
        case 0 => a._2.compareTo(b._2)
        case x => x
      }
    }

    override def groupCompare(a: (FirstName, LastName), b: (FirstName, LastName)): Int = {
      a._1.compareTo(b._1)
    }

  }
}

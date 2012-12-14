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
package testing
package mutable

import org.specs2.matcher.{ThrownExpectations, ThrownMessages}
import application._
import core._
import scalaz.syntax.bifunctor._
import scalaz.std.tuple._
/**
 * This trait helps in the creation of DLists and Scoobi jobs where the user doesn't have to track the creation of files.
 * All data is written to temporary files and is deleted after usage.
 */
trait SimpleJobs { outer =>

  implicit def asRunnableDList[T](list: DList[T]) = new RunnableDList(list)
  case class RunnableDList[T](list: DList[T]) {
    def run(implicit configuration: ScoobiConfiguration) = outer.run(list)
  }

  implicit def asRunnableDListPair[T, S](lists: (DList[T], DList[S])) = new RunnableDListPair(lists)
  case class RunnableDListPair[T, S](lists: (DList[T], DList[S])) {
    def run(implicit configuration: ScoobiConfiguration) = outer.run(lists._1, lists._2)
  }

  implicit def asRunnableDListSeq[T](lists: Seq[DList[T]]) = new RunnableDListSeq(lists)
  case class RunnableDListSeq[T, S](lists: Seq[DList[T]]) {
    def run(implicit configuration: ScoobiConfiguration) = outer.run(lists)
  }

  implicit def asRunnableDObject[T](o: DObject[T]) = new RunnableDObject(o)
  case class RunnableDObject[T](o: DObject[T]) {
    def run(implicit configuration: ScoobiConfiguration) = outer.run(o)
  }

  def run[T](list: =>DList[T])(implicit configuration: ScoobiConfiguration): Seq[T] =
    Vector(Persister.persist(list.materialise).toSeq:_*)

  def run[T, S](list1: DList[T], list2: DList[S])(implicit configuration: ScoobiConfiguration): (Seq[T], Seq[S]) =
    Persister.persist((list1.materialise, list2.materialise)).bimap((_.toSeq), (_.toSeq))

  def run[T](lists: Seq[DList[T]])(implicit configuration: ScoobiConfiguration): Seq[Seq[T]] =
    // E.T. I don't know how to avoid the asInstanceOf here :-(
    Persister.persist(lists.map(_.materialise)).asInstanceOf[Seq[Iterable[T]]].map(_.toSeq)

  def run[T](o: DObject[T])(implicit configuration: ScoobiConfiguration): T =
    Persister.persist(o)

  /**
   * @return a simple job from a list of strings (for the input file) and the current configuration
   */
  def fromInput(ts: String*)(implicit c: ScoobiConfiguration) =
    InputStringTestFile(ts).lines

  /**
   * @return a DList input keeping track of its temporary input file
   */
  def fromDelimitedInput(ts: String*)(implicit c: ScoobiConfiguration) =
    new InputTestFile[List[String]](ts, mapping = (_:String).split(",").toList).lines

  def fromKeyValues(ts: String*)(implicit c: ScoobiConfiguration): DList[(String, String)] =
    fromDelimitedInput(ts:_*).map { case k :: v :: _ => (k, v); case line => ("error", "could not split line "+line) }
}

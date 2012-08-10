package com.nicta.scoobi
package testing
package mutable

import org.specs2.matcher.{ThrownExpectations, ThrownMessages}
import application._
import core._
import scalaz.Scalaz._
/**
 * This trait helps in the creation of DLists and Scoobi jobs where the user doesn't have to track the creation of files.
 * All data is written to temporary files and is deleted after usage.
 */
trait SimpleJobs extends ThrownMessages { outer: ThrownExpectations with LocalHadoop =>

  implicit def asRunnableDList[T](list: DList[T]) = new RunnableDList(list)
  case class RunnableDList[T](list: DList[T]) {
    def run(implicit configuration: ScoobiConfiguration) = outer.run(list)
  }

  implicit def asRunnableDListPair[T, S](lists: (DList[T], DList[S])) = new RunnableDListPair(lists)
  case class RunnableDListPair[T, S](lists: (DList[T], DList[S])) {
    def run(implicit configuration: ScoobiConfiguration) = outer.run(lists._1, lists._2)
  }

  implicit def asRunnableDObject[T](o: DObject[T]) = new RunnableDObject(o)
  case class RunnableDObject[T](o: DObject[T]) {
    def run(implicit configuration: ScoobiConfiguration) = outer.run(o)
  }

  def run[T](list: =>DList[T])(implicit configuration: ScoobiConfiguration): Seq[T] =
    Persister.persist(list.materialize).toSeq

  def run[T, S](list1: DList[T], list2: DList[S])(implicit configuration: ScoobiConfiguration): (Seq[T], Seq[S]) =
    Persister.persist((list1.materialize, list2.materialize)).bimap((_.toSeq), (_.toSeq))

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
}

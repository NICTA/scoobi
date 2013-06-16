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

import scalaz._
import NonEmptyList._
import Zipper._

private[scoobi]
trait Seqs {

  /**
   * Split a sequence into smaller input splits according to a splitSize parameter.
   *
   * if the list is empty or with 1 element, don't try to group by splitSize because in this case splitSize is 0
   * and grouped will fail at runtime (see issue #60, issue #75)
   */
  def split[A, B, T <% BoundedLinearSeq[A]](seq: T, splitSize: Int, makeSplit: (Int, Int, T) => B): Seq[B] = {
    if      (seq.size == 0) Seq[B]()
    else if (seq.size == 1) Seq(makeSplit(0, seq.size, seq))
    else                    (0 to (seq.size - 1)).toSeq.grouped(splitSize).
                            map { r => (r.head, r.size) }.
                            map { case (s, l) => makeSplit(s, l, seq) }.toSeq
  }

  implicit def sequenceToSeqBoundedLinearSeq[A](seq: Seq[A]): BoundedLinearSeq[A] = SeqBoundedLinearSeq(seq)

  /** @return an extension for a seq */
  implicit def extendSeq[T](seq: Seq[T]): ExtendedSeq[T] = new ExtendedSeq(seq)
  /**
   * Additional methods for seqs
   */
  class ExtendedSeq[T](seq: Seq[T]) {

    /** update the last element if there is one */
    def updateLast(f: T => T) = seq match {
      case s :+ last => s :+ f(last)
      case other     => other
    }

    /** update the last element or start the sequence with a new init value */
    def updateLastOr(f: PartialFunction[T, T])(initValue: =>T) = seq match {
      case s :+ last => s :+ f(last)
      case other     => seq :+ initValue
    }

    /**
     * remove the first element satisfying the predicate
     * @return a seq minus the first element satisfying the predicate
     */
    def removeFirst(predicate: T => Boolean): Seq[T] = {
      val (withoutElement, startWithElement) = seq span (x => !predicate(x))
      withoutElement ++ startWithElement.drop(1)
    }

  }

  /**
   * @return a group elements of a sequence into groups so that for each element of a group there is at least another
   *         element in the same group verifying the predicate
   */
  def partitionWhen[A](seq: Seq[A])(predicate: (A, A) => Boolean): Seq[Seq[A]] =
    seq.foldLeft(Seq[Seq[A]]()) { (res, cur) =>
      val groups = res.toZipper(Seq())
      groups.findZor(group => group.isEmpty || group.exists(predicate(_, cur)),
                              groups.insertRight(Seq[A]()))
            .modify(_ :+ cur).toStream.toSeq
    }

  /** add a to Nel method on a sequence to turn it into a Nel, depending on the existence of 'head' */
  implicit class ToNel[A](seq: Seq[A]) {
    def toNel(default: =>A): NonEmptyList[A] = seq match {
      case head +: rest => nel(head, rest.toList)
      case _            => nel(default, Nil)
    }
  }
  /** add a toZipper method on a sequence to turn it into a Zipper, depending on the existence of 'head' */
  implicit class ToZipper[A](seq: Seq[A]) {
    def toZipper(default: =>A): Zipper[A] = seq.toNel(default).toZipper
  }

  /** function returning elements toString separated by a newline */
  val mkStrings = (seq: Seq[_]) => seq.mkString("\n")
}
/**
 * extrator for the first element of Seq[T]
 */
object +: {
  def unapply[T](l: Seq[T]): Option[(T, Seq[T])] = {
    if(l.isEmpty) None
    else          Some(l.head, l.tail)
  }
}

/**
 * extrator for the last element of Seq[T]
 */
object :+ {
  def unapply[T](l: Seq[T]): Option[(Seq[T], T)] = {
    if(l.isEmpty) None
    else          Some(l.init, l.last)
  }
}

trait BoundedLinearSeq[+A] {
  def apply(i: Int): A
  def size: Int
}

case class SeqBoundedLinearSeq[A](seq: Seq[A]) extends BoundedLinearSeq[A] {
  def apply(i: Int): A = seq(i)
  def size: Int        = seq.size
}

case class FunctionBoundedLinearSeq[A](f: Int => A, fsize: Int) extends BoundedLinearSeq[A] {
  def apply(i: Int): A = f(i)
  def size: Int        = fsize
}

private[scoobi]
object Seqs extends Seqs

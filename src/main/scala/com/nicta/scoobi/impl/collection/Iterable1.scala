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

/**
 * A non-empty iterable contains at least one element. Consequences include:
 *
 * - `reduceLeft` will always produce a value.
 * - `head` will always produce a value.
 * - `tail` will always produce a value.
 *
 * Some operations on a non-empty iterable result in a non-empty iterable.
 *
 * Construction of an `Iterable1` is typically performed with the `+::` method, defined on `Iterable1.RichIterator`.
 *
 * For example:
 *
 * {{{
 * import Iterable1._
 *
 * // A regular iterator.
 * val x: Iterable[Int] = ...
 * // Constructs a non-empty iterable with 74 at the head.
 * val y: Iterable1[Int] = 74 +:: x
 * }}}
 */
trait Iterable1[+A] {
  val head: A
  val tail: Iterable[A]

  import Iterator1._
  import Iterable1._

  /**
   * Return a non-empty iterator.
   */
  def iterator: Iterator1[A] =
    head +:: tail.iterator

  /**
   * Return a regular iterable, losing the non-empty invariant in the type.
   */
  def toIterable: Iterable[A] =
    new Iterable[A] {
      def iterator =
        Iterable1.this.iterator.toIterator
    }

  /**
   * Flatten an iterable of iterable.
   */
  def flatten[I](implicit I1: A => Iterable1[I]): Iterable1[I] = {
    val r = I1(head)
    r.head +:: (r.tail ++ tail.flatten(I1(_).iterator))
  }

  /**
   * The number of element in the iterable.
   */
  def size: Int =
    1 + tail.size

  /**
   * Append the given iterable to this iterable.
   */
  def ++[AA >: A](that: => Iterable1[AA]): Iterable1[AA] =
    head +:: (tail ++ that.toIterable)

  /**
   * Map a function on all elements of the iterable.
   */
  def map[B](f: A => B): Iterable1[B] =
    f(head) +:: (tail map f)

  /**
   * Sequence an iterable function on all elements of the iterable.
   */
  def flatMap[B](f: A => Iterable1[B]): Iterable1[B] = {
    val k = f(head)
    k.head +:: (k.tail ++ (tail flatMap (f(_).toIterable)))
  }

  /**
   * Runs the iterable sequence effect (`flatMap`) of functions on this iterable.
   */
  def <*>:[B](f: Iterable1[A => B]): Iterable1[B] =
    f flatMap (map(_))

  /**
   * Zip this iterable with the given iterable to produce an iterable of pairs.
   */
  def zip[B](b: Iterable1[B]): Iterable1[(A, B)] =
    (head, b.head) +:: (tail zip b.tail)

  /**
   * Zip this iterable with the infinite iterable from 0 incrementing by 1, to produce an iterable of pairs.
   */
  def zipWithIndex: Iterable1[(A, Int)] =
    (head, 0) +:: (tail.zipWithIndex map {
      case (a, n) => (a, n + 1)
    })

  /**
   * Creates an iterable formed from this iterable and another iterable by combining corresponding elements in pairs.
   */
  def zipAll[B, AA >: A, BB >: B](that: Iterable1[B], thisElem: AA, thatElem: BB): Iterable1[(AA, BB)] =
    (head, that.head) +:: (tail zipAll (that.tail, thisElem, thatElem))

  /**
   * Return an iterable with only the elements satisfying the predicate.
   */
  def filter(p: A => Boolean): Iterable[A] =
    toIterable filter p

  /**
   * Partition the iterable into those satisfying a predicate and those that do not.
   */
  def partition(p: A => Boolean): BreakIterable1[A] = {
    val (xx, yy) = tail partition p
    if(p(head))
      LeftBreakIterable1(head +:: xx, yy)
    else
      RightBreakIterable1(xx, head +:: yy)
  }

  /**
   * Execute the given effect on each element of the iterable.
   */
  def foreach[U](f: A => U) = {
    f(head)
    tail foreach f
  }

  /**
   * True if all elements of the iterable satisfy the given predicate.
   */
  def forall(p: A => Boolean): Boolean =
    p(head) && (tail forall p)

  /**
   * True if any elements of the iterable satisfy the given predicate.
   */
  def exists(p: A => Boolean): Boolean =
    p(head) || (tail exists p)

  /**
   * Return the number of elements satisfying the given predicate.
   */
  def count(p: A => Boolean): Int = {
    var c = 0
    foreach(a => if(p(a)) c += 1)
    c
  }

  /**
   * Return the first element in the iterable satisfying the given predicate.
   */
  def find(p: A => Boolean): Option[A] =
    if(p(head))
      Some(head)
    else
      tail find p

  /**
   * Return the first element in the iterable satisfying the given predicate, mapping the given function.
   */
  def collectFirst[B](pf: PartialFunction[A, B]): Option[B] = {
    for (x <- iterator) {
      if (pf isDefinedAt x)
        return Some(pf(x))
    }
    None
  }

  /**
   * Reduce the iterable using the given seed and loop function. Synonym for `/:`.
   */
  def foldLeft[B](b: B)(op: (B, A) => B): B =
    toIterable.foldLeft(b)(op)

  /**
   * Reduce the iterable using the given seed and loop function. Synonym for `foldLeft`.
   */
  def /:[B](b: B)(op: (B, A) => B): B =
    foldLeft(b)(op)

  /**
   * Reduce the iterable seeded with the head and the loop function on the tail.
   */
  def reduceLeft[AA >: A](op: (AA, A) => AA): AA =
    tail.foldLeft[AA](head)(op)

  /**
   * Produces an iterable containing cumulative results of applying the operator going left to right.
   */
  def scanLeft[B](z: B)(op: (B, A) => B): Iterable1[B] =
    toIterable.scan1Left(z)(op)

  /**
   * Produces an iterable containing cumulative results of applying the operator going left to right, starting at head.
   */
  def scanLeft1[AA >: A](op: (AA, A) => AA): Iterable1[AA] = {
    var acc: AA = head
    val b = new scala.collection.mutable.ListBuffer[AA]
    foreach { x =>
      acc = op(acc, x)
      b += acc
    }

    head +:: b
  }

  /**
   * Take at most the given number of elements from the front of the iterable.
   */
  def take(n: Int): Iterable[A] =
    toIterable take n

  /**
   * Drop at most the given number of elements from the front of the iterable.
   */
  def drop(n: Int): Iterable[A] =
    toIterable drop n

  /**
   * Returns an interval of elements in the iterable.
   */
  def slice(from: Int, to: Int): Iterable[A] =
    toIterable slice (from, to)

  /**
   * Take elements from the front of the iterable satisfying the given predicate.
   */
  def takeWhile(p: A => Boolean): Iterable[A] =
    toIterable takeWhile p

  /**
   * Drop elements from the front of the iterable satisfying the given predicate.
   */
  def dropWhile(p: A => Boolean): Iterable[A] =
    toIterable dropWhile p

  /**
   * Split the iterable, taking from the front while the given predicate satisfies.
   */
  def span(p: A => Boolean): BreakIterable1[A] = {
    if(p(head)) {
      val (xx, yy) = tail span p
      LeftBreakIterable1(head +:: xx, yy)
    } else
      RightBreakIterable1(Iterable.empty, head +:: tail)
  }

  /**
   * Convert this iterable to a list.
   */
  def toList: List[A] =
    toIterable.toList

  /**
   * Convert this iterable to a seq.
   */
  def toSeq: Seq[A] =
    toIterable.toSeq

  /**
   * Convert this iterable to a stream.
   */
  def toStream: Stream[A] =
    toIterable.toStream

  override def toString: String = {
    val i = iterator
    val b = new StringBuilder
    b += '['

    while(i.hasNext) {
      val e = i.next
      b append e
      if(i.hasNext)
        b += ','
    }

    b += ']'

    b.toString
  }

  /**
   * Runs the iterable of functions to produce a function to iterables.
   */
  def sequenceFn[X, Y](implicit I: A => X => Y): X => Iterable1[Y] =
    x =>
      map(I(_)(x))

  override def equals(a: Any): Boolean =
    a.isInstanceOf[Iterable1[_]] && {
      val aa = a.asInstanceOf[Iterable1[_]]
      head == aa.head && tail == aa.tail
    }

  override def hashCode: Int =
    head.hashCode + tail.hashCode
}

object Iterable1 {
  /**
   * Add methods to `scala.collection.Iterable[A]`.
   */
  case class RichIterable[+A](it: Iterable[A]) {
    /**
     * Prepends an element to a regular iterable to produce a non-empty iterable.
     */
    def +::[AA >: A](h: AA): Iterable1[AA] =
      new Iterable1[AA] {
        val head = h
        val tail = it
      }

    /**
     * Produces an iterable containing cumulative results of applying the operator going left to right.
     */
    def scan1Left[B](z: B)(op: (B, A) => B): Iterable1[B] = {
      val h = z
      var acc = z
      val b = new scala.collection.mutable.ListBuffer[B]
      it.foreach { x =>
        acc = op(acc, x)
        b += acc
      }

      h +:: b
    }

  }

  implicit def IterableToIterable1[A](it: Iterable[A]): RichIterable[A] =
    RichIterable(it)

  /**
   * A return type used in `Iterable1` functions that split an iterable. Splitting a non-empty iterable will result in a non-empty iterable and a regular iterable.
   *
   * Isomorphic to '[A](Boolean, Iterable1[A], Iterable[A])'.
   */
  sealed trait BreakIterable1[+A]

  /**
   * The iterable split in two with non-empty first.
   */
  case class LeftBreakIterable1[+A](x: Iterable1[A], y: Iterable[A]) extends BreakIterable1[A]

  /**
   * The iterable split in two with non-empty second.
   */
  case class RightBreakIterable1[+A](x: Iterable[A], y: Iterable1[A]) extends BreakIterable1[A]
  /**
   * Construct an iterable with a single value.
   */
  def single[A](elem: A): Iterable1[A] =
    elem +:: Iterable.empty

  /**
   * Construct an iterable with the given first and rest of values.
   */
  def apply[A](elem: A, elems: A*): Iterable1[A] =
    elem +:: Iterable(elems: _*)

  /**
   * Produce an infinite iterable, starting at the given seed and applying the given transformation continually.
   */
  def iterate[A](start: A)(f: A => A): Iterable1[A] =
    start +:: (new Iterable[A] {
      def iterator =
        Iterator.iterate(f(start))(f)
    })

  /**
   * Produce an infinite iterable starting at the given integer and incrementing by 1 continually.
   */
  def from(start: Int): Iterable1[Int] =
    iterate(start)(_ + 1)

  /**
   * Produce an infinite iterable starting at the given integer and incrementing by the given step continually.
   */
  def from(start: Int, step: Int): Iterable1[Int] =
    iterate(start)(_ + step)

  implicit def Iterable1WireFormat[A: core.WireFormat]: core.WireFormat[Iterable1[A]] = {
    val is = implicitly[core.WireFormat[Iterable[A]]]
    val i  = implicitly[core.WireFormat[A]]
    i *** is xmap (e => e._1 +:: e._2, (q: Iterable1[A]) => (q.head, q.tail))
  }
}

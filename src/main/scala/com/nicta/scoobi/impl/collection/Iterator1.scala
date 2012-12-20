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
 * A non-empty iterator contains at least one element. Consequences include:
 *
 * - `reduceLeft` will always produce a value.
 * - `first` will always produce a value.
 * - `next` will always produce a value on its first invocation.
 * - `hasNext` will always return true on its first invocation.
 * - `scanLeft1` will always produce a value.
 *
 * Some operations on a non-empty iterator result in a non-empty iterator.
 *
 * Construction of an `Iterator1` is typically performed with the `+::` method, defined on `Iterator1.RichIterator`.
 *
 * For example:
 *
 * {{{
 * import Iterator1._
 *
 * // A regular iterator.
 * val x: Iterator[Int] = ...
 * // Constructs a non-empty iterator with 74 at the first.
 * val y: Iterator1[Int] = 77 +:: x
 * }}}
 *
 * '''NOTE: Most Iterator functions perform SIDE-EFFECTS and so EQUATIONAL REASONING DOES NOT APPLY.'''
 */
trait Iterator1[+A] extends TraversableOnce[A] {
  /**
   * The constant first element of this iterator.
   */
  def first: A
  private[collection] val rest: Iterator[A]

  import Iterator1._

  private var fnext: Boolean = false

  /**
   * True if this iterator can produce a value from `next`.
   */
  def hasNext: Boolean =
    !fnext || rest.hasNext

  /**
   * Produces a value if possible, or throws an exception if not possible. The method `hasNext` determines if a value can be produced.
   */
  def next: A =
    if(fnext)
      rest.next
    else {
      fnext = true
      first
    }

  /**
   * Return a regular iterator, losing the non-empty invariant in the type.
   */
  def seq: Iterator[A] =
    toIterator

  /**
   * Returns a stream of this iterator.
   */
  def toTraversable: Traversable[A] =
    toStream

  /**
   * Copies this iterator to an array at the given interval.
   */
  def copyToArray[AA >: A](xs: Array[AA], start: Int, n: Int): Unit = {
    var i = start
    val end = math.min(start + n, xs.length)
    while (hasNext && i < end) {
      xs(i) = next
      i += 1
    }
  }

  /**
   * A non-empty iterator always has a definite size.
   */
  def hasDefiniteSize: Boolean =
    false

  /**
   * A non-empty iterator is never traversable again.
   */
  def isTraversableAgain: Boolean =
    false

  /**
   * Return a regular iterator, losing the non-empty invariant in the type.
   */
  def toIterator: Iterator[A] =
    new Iterator[A] {
      def hasNext =
        Iterator1.this.hasNext

      def next =
        Iterator1.this.next
    }

  /**
   * The negation of `hasNext`.
   */
  def isEmpty =
    !hasNext

  /**
   * Take at most the given number of elements from the front of the iterator.
   */
  def take(n: Int): Iterator[A] =
    toIterator take n

  /**
   * Drop at most the given number of elements from the front of the iterator.
   */
  def drop(n: Int): Iterator[A] =
    toIterator drop n

  /**
   * Returns an interval of elements in the iterator.
   */
  def slice(from: Int, to: Int): Iterator[A] =
    toIterator slice (from, to)

  /**
   * Map a function on all elements of the iterator.
   */
  def map[B](f: A => B): Iterator1[B] =
    f(first) +:: (rest map f)

  /**
   * Append the given iterator to this iterator.
   */
  def ++[AA >: A](that: => Iterator1[AA]): Iterator1[AA] =
    first +:: (rest ++ that.toIterator)

  /**
   * Sequence an iterator function on all elements of the iterator.
   */
  def flatMap[B](f: A => Iterator1[B]): Iterator1[B] = {
    val k = f(first)
    k.first +:: (k.rest ++ (rest flatMap (f(_).toIterator)))
  }

  /**
   * Flatten an iterator of iterator.
   */
  def flatten[I](implicit I1: A => Iterator1[I]): Iterator1[I] =
    flatMap(I1)

  /**
   * Runs the iterator sequence effect (`flatMap`) of functions on this iterator. Synonym for `<*>:`.
   */
  def ap[B](f: Iterator1[A => B]): Iterator1[B] =
    f flatMap (map(_))

  /**
   * Runs the iterator sequence effect (`flatMap`) of functions on this iterator. Synonym for `ap`.
   */
  def <*>:[B](f: Iterator1[A => B]): Iterator1[B] =
    ap(f)

  /**
   * Return an iterator with only the elements satisfying the predicate.
   */
  def filter(p: A => Boolean): Iterator[A] =
    toIterator filter p

  /**
   * Return an iterator with only the elements satisfying the predicate.
   */
  def withFilter(p: A => Boolean): Iterator[A] =
    toIterator withFilter p

  /**
   * Return an iterator with only the elements not satisfying the predicate.
   */
  def filterNot(p: A => Boolean): Iterator[A] =
    toIterator filterNot p

  /**
   * Return the first element in the iterator satisfying the given predicate, mapping the given function.
   */
  def collect[B](pf: PartialFunction[A, B]): Iterator[B] =
    toIterator collect pf

  /**
   * Take elements from the front of the iterator satisfying the given predicate.
   */
  def takeWhile(p: A => Boolean): Iterator[A] =
    toIterator takeWhile p

  /**
   * Partition the iterator into those satisfying a predicate and those that do not.
   */
  def partition(p: A => Boolean): BreakIterator1[A] = {
    val (xx, yy) = rest partition p
    if(p(first))
      LeftBreakIterator1(first +:: xx, yy)
    else
      RightBreakIterator1(xx, first +:: yy)
  }

  /**
   * Split the iterator, taking from the front while the given predicate satisfies.
   */
  def span(p: A => Boolean): BreakIterator1[A] = {
    if(p(first)) {
      val (xx, yy) = rest span p
      LeftBreakIterator1(first +:: xx, yy)
    } else
      RightBreakIterator1(Iterator.empty, first +:: rest)
  }

  /**
   * Drop elements from the front of the iterator satisfying the given predicate.
   */
  def dropWhile(p: A => Boolean): Iterator[A] =
    toIterator dropWhile p

  /**
   * Zip this iterator with the given iterator to produce an iterator of pairs.
   */
  def zip[B](that: Iterator1[B]): Iterator1[(A, B)] =
    (first, that.first) +:: (rest zip that.rest)

  /**
   * Appends an element value to the iterator until a given target length is reached.
   */
  def padTo[AA >: A](len: Int, elem: AA): Iterator1[AA] =
    first +:: (rest padTo (len - 1, elem))

  /**
   * Zip this iterator with the infinite iterator from 0 incrementing by 1, to produce an iterator of pairs.
   */
  def zipWithIndex: Iterator1[(A, Int)] =
    (first, 0) +:: (rest.zipWithIndex map {
      case (a, n) => (a, n + 1)
    })

  /**
   * Creates an iterator formed from this iterator and another iterator by combining corresponding elements in pairs.
   */
  def zipAll[B, AA >: A, BB >: B](that: Iterator1[B], thisElem: AA, thatElem: BB): Iterator1[(AA, BB)] =
    (first, that.first) +:: (rest zipAll (that.rest, thisElem, thatElem))

  /**
   * Execute an effect for each element of the iterator.
   */
  def foreach[U](f: A => U) = {
    f(first)
    rest foreach f
  }

  /**
   * True if all elements of the iterator satisfy the given predicate.
   */
  def forall(p: A => Boolean): Boolean =
    p(first) && (rest forall p)

  /**
   * True if any elements of the iterator satisfy the given predicate.
   */
  def exists(p: A => Boolean): Boolean =
    p(first) || (rest exists p)

  /**
   * True if any elements of the iterator are equal to the given value.
   */
  def contains(elem: Any): Boolean =
    first == elem || (rest contains elem)

  /**
   * Return the first element in the iterator satisfying the given predicate.
   */
  def find(p: A => Boolean): Option[A] =
    if(p(first))
      Some(first)
    else
      rest find p

  /**
   * Produces an iterator containing cumulative results of applying the operator going left to right.
   */
  def scanLeft[B](z: B)(op: (B, A) => B): Iterator1[B] =
    toIterator.scan1Left(z)(op)

  /**
   * Produces an iterator containing cumulative results of applying the operator going left to right, starting at head.
   */
  def scanLeft1[AA >: A](op: (AA, A) => AA): Iterator1[AA] = {
    var acc: AA = first
    val b = new scala.collection.mutable.ListBuffer[AA]
    foreach { x =>
      acc = op(acc, x)
      b += acc
    }

    first +:: b.iterator
  }

  /**
   * Return the index (starting at 0) of the first element in the iterator satisfying the given predicate or -1 if no such element exists.
   */
  def indexWhere(p: A => Boolean): Int =
    if(p(first))
      0
    else {
      val i = rest indexWhere p
      if(i == -1)
        -1
      else
        i + 1
    }

  /**
   * Return the index (starting at 0) of the first element in the iterator equal to the given value or -1 if no such element exists.
   */
  def indexOf[AA >: A](elem: AA): Int =
    if(first == elem)
      0
    else {
      val i = rest indexOf elem
      if(i == -1)
        -1
      else
        i + 1
    }

  /**
   * Return the number of elements in the iterator.
   */
  def length: Int =
    1 + rest.length

  /**
   * Creates two new iterators that both iterate over the same elements as this iterator (in the same order).  The duplicate iterators are considered equal if they are positioned at the same element.
   */
  def duplicate: (Iterator1[A], Iterator1[A]) = {
    val (x, y) = rest.duplicate
    (first +:: x, first +:: y)
  }

  /**
   * True if this iterator produces equal elements to the given iterator in the same order.
   */
  def sameElements(that: Iterator1[_]): Boolean =
    (first == that.first) && (rest sameElements that.rest)

  /**
   * Convert this iterator to a stream.
   */
  def toStream: Stream[A] =
    first #:: rest.toStream

  /**
   * Runs the iterator of functions to produce a function to iterators.
   */
  def sequenceFn[X, Y](implicit I: A => X => Y): X => Iterator1[Y] =
    x =>
      map(I(_)(x))

  override def toString =
    "non-empty iterator (Iterator1)"

}

object Iterator1 {
  // CAUTION
  private[collection] def unsafeIterator1[A](it: Iterator[A]): Iterator1[A] =
    if(it.hasNext) {
      val h = it.next
      h +:: it
    } else
      sys.error("Invariant broken. Iterator1#unsafeIterator1 was invoked on an empty Iterator.")

  /**
   * Add methods to `scala.collection.Iterable[A]`.
   */
  case class RichIterator[+A](it: Iterator[A]) {
    /**
     * Prepends an element to a regular iterator to produce a non-empty iterator.
     */
    def +::[AA >: A](h: AA): Iterator1[AA] =
      new Iterator1[AA] {
        def first = h
        val rest = it
      }

    /**
     * Produces an iterator containing cumulative results of applying the operator going left to right.
     */
    def scan1Left[B](z: B)(op: (B, A) => B): Iterator1[B] =
      unsafeIterator1(it.scanLeft(z)(op))

  }

  implicit def IteratorToIterator1[A](it: Iterator[A]): RichIterator[A] =
    RichIterator(it)

  /**
   * A return type used in `Iterator` functions that split an iterator. Splitting a non-empty iterator will result in a non-empty iterator and a regular iterator.
   *
   * Isomorphic to '[A](Boolean, Iterator1[A], Iterator[A])'.
   */
  sealed trait BreakIterator1[+A]

  /**
   * The iterator split in two with non-empty first.
   */
  case class LeftBreakIterator1[+A](x: Iterator1[A], y: Iterator[A]) extends BreakIterator1[A]

  /**
   * The iterator split in two with non-empty second.
   */
  case class RightBreakIterator1[+A](x: Iterator[A], y: Iterator1[A]) extends BreakIterator1[A]

  /**
   * Construct an iterator with a single value.
   */
  def single[A](elem: A): Iterator1[A] =
    elem +:: Iterator.empty

  /**
   * Construct an iterator with the given first and rest of values.
   */
  def apply[A](elem: A, elems: A*): Iterator1[A] =
    elem +:: Iterator(elems: _*)

  /**
   * Produce an infinite iterator, starting at the given seed and applying the given transformation continually.
   */
  def iterate[A](start: A)(f: A => A): Iterator1[A] =
    start +:: Iterator.iterate(f(start))(f)

  /**
   * Produce an infinite iterator starting at the given integer and incrementing by 1 continually.
   */
  def from(start: Int): Iterator1[Int] =
    start +:: Iterator.from(start + 1)

  /**
   * Produce an infinite iterator starting at the given integer and incrementing by the given step continually.
   */
  def from(start: Int, step: Int): Iterator1[Int] =
    start +:: Iterator.from(start + step)

}

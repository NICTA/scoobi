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

import java.lang.ref.{WeakReference, ReferenceQueue}
import scala.annotation.tailrec
import scala.collection.mutable.{Set => mSet}
import WeakHashSet._


/**
 * A HashSet where the elements are stored weakly. Elements in this set are eligible for GC if no other
 * hard references are associated with them. Its primary use case is as a canonical reference
 * identity holder (aka "hash-consing") via findEntryOrUpdate
 *
 * This Set implementation cannot hold null. Any attempt to put a null in it will result in a NullPointerException
 *
 * This set implementation is not in general thread safe without external concurrency control. However it behaves
 * properly when GC concurrently collects elements in this set.
 *
 *  Copied from the scala.reflect.internal.util code but without the AnyRef constraint
 */
final class WeakHashSet[A](val initialCapacity: Int = WeakHashSet.defaultInitialCapacity, val loadFactor: Double = WeakHashSet.defaultLoadFactor) extends Set[A] with Function1[A, Boolean] with mSet[A] {

  type This = WeakHashSet[A]

  /**
   * queue of Entries that hold elements scheduled for GC
   * the removeStaleEntries() method works through the queue to remove
   * stale entries from the table
   */
  private[this] val queue = new ReferenceQueue[A]

  /**
   * the number of elements in this set
   */
  private[this] var count = 0

  /**
   * from a specified initial capacity compute the capacity we'll use as being the next
   * power of two equal to or greater than the specified initial capacity
   */
  private def computeCapacity = {
    if (initialCapacity < 0) throw new IllegalArgumentException("initial capacity cannot be less than 0")
    var candidate = 1
    while (candidate < initialCapacity) {
      candidate  *= 2
    }
    candidate
  }

  /**
   * the underlying table of entries which is an array of Entry linked lists
   */
  private[this] var table = new Array[Entry[A]](computeCapacity)

  /**
   * the limit at which we'll increase the size of the hash table
   */
  var threshhold = computeThreshHold

  private[this] def computeThreshHold: Int = (table.size * loadFactor).ceil.toInt

  /**
   * find the bucket associated with an element's hash code
   */
  private[this] def bucketFor(hash: Int): Int = {
    // spread the bits around to try to avoid accidental collisions using the
    // same algorithm as java.util.HashMap
    var h = hash
    h ^= h >>> 20 ^ h >>> 12
    h ^= h >>> 7 ^ h >>> 4

    // this is finding h % table.length, but takes advantage of the
    // fact that table length is a power of 2,
    // if you don't do bit flipping in your head, if table.length
    // is binary 100000.. (with n 0s) then table.length - 1
    // is 1111.. with n 1's.
    // In other words this masks on the last n bits in the hash
    h & (table.length - 1)
  }

  /**
   * remove a single entry from a linked list in a given bucket
   */
  private[this] def remove(bucket: Int, prevEntry: Entry[A], entry: Entry[A]) {
    prevEntry match {
      case null => table(bucket) = entry.tail
      case _ => prevEntry.tail = entry.tail
    }
    count -= 1
  }

  /**
   * remove entries associated with elements that have been gc'ed
   */
  private[this] def removeStaleEntries() {
    def poll(): Entry[A] = queue.poll().asInstanceOf[Entry[A]]

    @tailrec
    def queueLoop {
      val stale = poll()
      if (stale != null) {
        val bucket = bucketFor(stale.hash)

        @tailrec
        def linkedListLoop(prevEntry: Entry[A], entry: Entry[A]): Unit = if (stale eq entry) remove(bucket, prevEntry, entry)
        else if (entry != null) linkedListLoop(entry, entry.tail)

        linkedListLoop(null, table(bucket))

        queueLoop
      }
    }

    queueLoop
  }

  /**
   * Double the size of the internal table
   */
  private[this] def resize() {
    val oldTable = table
    table = new Array[Entry[A]](oldTable.size * 2)
    threshhold = computeThreshHold

    @tailrec
    def tableLoop(oldBucket: Int): Unit = if (oldBucket < oldTable.size) {
      @tailrec
      def linkedListLoop(entry: Entry[A]): Unit = entry match {
        case null => ()
        case _ => {
          val bucket = bucketFor(entry.hash)
          val oldNext = entry.tail
          entry.tail = table(bucket)
          table(bucket) = entry
          linkedListLoop(oldNext)
        }
      }
      linkedListLoop(oldTable(oldBucket))

      tableLoop(oldBucket + 1)
    }
    tableLoop(0)
  }

  // from scala.reflect.internal.Set, find an element or null if it isn't contained
  override def findEntry(elem: A): A = elem match {
    case null => throw new NullPointerException("WeakHashSet cannot hold nulls")
    case _    => {
      removeStaleEntries()
      val hash = elem.hashCode
      val bucket = bucketFor(hash)

      @tailrec
      def linkedListLoop(entry: Entry[A]): A = entry match {
        case null                    => null.asInstanceOf[A]
        case _                       => {
          val entryElem = entry.get
          if (elem == entryElem) entryElem
          else linkedListLoop(entry.tail)
        }
      }

      linkedListLoop(table(bucket))
    }
  }
  // add an element to this set unless it's already in there and return the element
  def findEntryOrUpdate(elem: A): A = elem match {
    case null => throw new NullPointerException("WeakHashSet cannot hold nulls")
    case _    => {
      removeStaleEntries()
      val hash = elem.hashCode
      val bucket = bucketFor(hash)
      val oldHead = table(bucket)

      def add() = {
        table(bucket) = new Entry(elem, hash, oldHead, queue)
        count += 1
        if (count > threshhold) resize()
        elem
      }

      @tailrec
      def linkedListLoop(entry: Entry[A]): A = entry match {
        case null                    => add()
        case _                       => {
          val entryElem = entry.get
          if (elem == entryElem) entryElem
          else linkedListLoop(entry.tail)
        }
      }

      linkedListLoop(oldHead)
    }
  }

  // add an element to this set unless it's already in there and return this set
  override def +(elem: A): this.type = elem match {
    case null => throw new NullPointerException("WeakHashSet cannot hold nulls")
    case _    => {
      removeStaleEntries()
      val hash = elem.hashCode
      val bucket = bucketFor(hash)
      val oldHead = table(bucket)

      def add() {
        table(bucket) = new Entry(elem, hash, oldHead, queue)
        count += 1
        if (count > threshhold) resize()
      }

      @tailrec
      def linkedListLoop(entry: Entry[A]): Unit = entry match {
        case null                      => add()
        case _ if (elem == entry.get) => ()
        case _                         => linkedListLoop(entry.tail)
      }

      linkedListLoop(oldHead)
      this
    }
  }

  def +=(elem: A) = this + elem

  // from scala.reflect.interanl.Set
  override def addEntry(x: A) { this += x }

  // remove an element from this set and return this set
  override def -(elem: A): this.type = elem match {
    case null => this
    case _ => {
      removeStaleEntries()
      val bucket = bucketFor(elem.hashCode)

      @tailrec
      def linkedListLoop(prevEntry: Entry[A], entry: Entry[A]): Unit = entry match {
        case null => ()
        case _ if (elem == entry.get) => remove(bucket, prevEntry, entry)
        case _ => linkedListLoop(entry, entry.tail)
      }

      linkedListLoop(null, table(bucket))
      this
    }
  }

  def -=(elem: A) = this - elem

  // empty this set
  override def clear(): Unit = {
    table = new Array[Entry[A]](table.size)
    threshhold = computeThreshHold
    count = 0

    // drain the queue - doesn't do anything because we're throwing away all the values anyway
    @tailrec def queueLoop(): Unit = if (queue.poll() != null) queueLoop()
    queueLoop()
  }

  // true if this set is empty
  override def empty: This = new WeakHashSet[A](initialCapacity, loadFactor)

  // the number of elements in this set
  override def size: Int = {
    removeStaleEntries()
    count
  }

  override def apply(x: A): Boolean = this contains x

  override def foreach[U](f: A => U): Unit = iterator foreach f

  override def toList(): List[A] = iterator.toList

  // Iterator over all the elements in this set in no particular order
  override def iterator: Iterator[A] = {
    removeStaleEntries()

    new Iterator[A] {

      /**
       * the bucket currently being examined. Initially it's set past the last bucket and will be decremented
       */
      private[this] var currentBucket: Int = table.size

      /**
       * the entry that was last examined
       */
      private[this] var entry: Entry[A] = null

      /**
       * the element that will be the result of the next call to next()
       */
      private[this] var lookaheadelement: A = null.asInstanceOf[A]

      @tailrec
      def hasNext: Boolean = {
        while (entry == null && currentBucket > 0) {
          currentBucket -= 1
          entry = table(currentBucket)
        }

        if (entry == null) false
        else {
          lookaheadelement = entry.get
          if (lookaheadelement == null) {
            // element null means the weakref has been cleared since we last did a removeStaleEntries(), move to the next entry
            entry = entry.tail
            hasNext
          } else {
            true
          }
        }
      }

      def next(): A = if (lookaheadelement == null)
        throw new IndexOutOfBoundsException("next on an empty iterator")
      else {
        val result = lookaheadelement
        lookaheadelement = null.asInstanceOf[A]
        entry = entry.tail
        result
      }
    }
  }
}

/**
 * Companion object for WeakHashSet
 */
object WeakHashSet {
  /**
   * A single entry in a WeakHashSet. It's a WeakReference plus a cached hash code and
   * a link to the next Entry in the same bucket
   */
  private class Entry[A](element: A, val hash:Int, var tail: Entry[A], queue: ReferenceQueue[A]) extends WeakReference[A](element, queue)

  val defaultInitialCapacity = 16
  val defaultLoadFactor = .75

  def apply[A <: AnyRef](initialCapacity: Int = WeakHashSet.defaultInitialCapacity, loadFactor: Double = WeakHashSet.defaultLoadFactor) = new WeakHashSet[A](initialCapacity, defaultLoadFactor)
}

/** A common class for lightweight sets.
 */
abstract class Set[T] {

  def findEntry(x: T): T

  def addEntry(x: T): Unit

  def iterator: Iterator[T]

  def foreach[U](f: T => U): Unit = iterator foreach f

  def apply(x: T): Boolean = contains(x)

  def contains(x: T): Boolean =
    findEntry(x) != null

  def toList = iterator.toList

}

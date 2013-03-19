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
package reactive

/**
 * An EventStream that is implemented by delegating everything to an EventSource
 */
trait EventStreamSourceProxy[T] extends EventStream[T] with Observing {
  lazy val source: EventSource[T] = new EventSource[T]

  def flatMap[U](f: T=>EventStream[U]): EventStream[U] = source.flatMap[U](f)
  def foreach(f: T=>Unit)(implicit observing: Observing): Unit = source.foreach(f)(observing)
  def |[U>:T](that: EventStream[U]): EventStream[U] = source.|(that)
  def map[U](f: T=>U): EventStream[U] = source.map(f)
  def filter(f: T=>Boolean): EventStream[T] = source.filter(f)
  def collect[U](pf: PartialFunction[T,U]): EventStream[U] = source.collect(pf)
  def takeWhile(p: T=>Boolean): EventStream[T] = source.takeWhile(p)
  def foldLeft[U](initial: U)(f: (U,T)=>U): EventStream[U] = source.foldLeft(initial)(f)
  def hold[U>:T](init: U): Signal[U] = source.hold(init)
  def nonrecursive: EventStream[T] = source.nonrecursive
  def debugString = source.debugString
  def debugName = source.debugName
  def zipWithStaleness = source.zipWithStaleness
  def nonblocking = source.nonblocking
  def distinct = source.distinct
  def throttle(period: Long): EventStream[T] = source.throttle(period)

  private[reactive] def addListener(f: (T) => Unit): Unit = source.addListener(f)
  private[reactive] def removeListener(f: (T) => Unit): Unit = source.removeListener(f)
}


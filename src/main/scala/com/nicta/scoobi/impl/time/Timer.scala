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
package time


import java.util.Calendar
import text.Plural._
import control.Exceptions._

/**
 * This trait provides Timer functionalities based on the Java Calendar milliseconds
 */
private[scoobi]
trait HmsTimer[T <: HmsTimer[T]] {
  /** elapsed times since for each stop */
  protected val elapsedTimes: List[Long] = Nil
  /** each time the timer is started we add the current time to this list of times number of millis when instantiating the object using this Trait */
  val startedTimestamps: List[Long] = Nil

  def copy(elapsed: List[Long], started: List[Long]): T
  /**
   * starts the with new elapsed time
   */
  def start = copy(Nil, getTime :: startedTimestamps)

  /**
   * restarts the Timer with no elapsed time
   */
  def restart = copy(Nil, Nil)

  /**
   * Stop the timer, store the number of elapsed millis and return a String representing the time as hour/minute/second/ms
   * @return the elapsed time as a String
   */
  def stop = copy((getTime - lastTimestamp) :: elapsedTimes, startedTimestamps.drop(1))

  /** add 2 timers together */
  def add(t: HmsTimer[T]) = {
    copy((elapsedTimes ++ t.elapsedTimes).filterNot(_ == 0), startedTimestamps ++ t.startedTimestamps)
  }

  /** @return true if this timer has been started */
  def isStarted = !startedTimestamps.isEmpty
  /** @return true if this timer has never been started */
  def neverStarted = !isStarted && elapsedTimes.isEmpty

  def totalMillis =
    if (isStarted) lastTimestamp - firstTimestamp
    else           elapsedTimes.sorted.lastOption.getOrElse(0L)

  private def lastTimestamp = startedTimestamps.sorted.lastOption.getOrElse(0L)
  private def firstTimestamp = startedTimestamps.sorted.headOption.getOrElse(0L)
  /**
   * @return a tuple with the elapsed hours, minutes, seconds and millis
   */
  def hourMinutesSecondsMillis = {
    val hours = totalMillis / 1000 / 3600
    val totalMillis1 = totalMillis - hours * 3600 * 1000
    val minutes = totalMillis1 / 1000 / 60
    val totalMillis2 = totalMillis1 - minutes * 60 * 1000
    val seconds = totalMillis2 / 1000
    val millis = totalMillis2 - seconds * 1000
    (hours, minutes, seconds, millis)
  }

  /**
   * @return a formatted string showing the hours, minutes and seconds
   */
  def hms: String = {
    val (hours, minutes, seconds, millis) = hourMinutesSecondsMillis
    var result = ""
    if (hours > 0) { result += hours + " hour".plural(hours) + " " }
    if (minutes > 0) { result += minutes + " minute".plural(minutes) + " " }
    result += (seconds + " second".plural(seconds))
    result
  }

  /**
   * @return a formatted string showing the hours, minutes, seconds and millis
   */
  def time: String = {
    val (hours, minutes, seconds, millis) = hourMinutesSecondsMillis
    (if (hms != "0 second") hms + ", " else "") +
      millis + " ms"
  }

  /**
   * this method can be overriden for testing
   */
  protected def getTime = Calendar.getInstance.getTime.getTime
}

private[scoobi]
class SimpleTimer extends HmsTimer[SimpleTimer] {
  def copy(e: List[Long] = Nil, m: List[Long] = Nil) =
    new SimpleTimer {
      override protected val elapsedTimes = e
      override val startedTimestamps = m
    }

  override def toString = hms

  override def equals(a: Any) = a match {
    case s: SimpleTimer => true
    case other          => false
  }
}

private[scoobi]
object SimpleTimer {
  def fromString(s: String) = new SimpleTimer {
    override protected val elapsedTimes = tryOrElse(List(java.lang.Long.parseLong(s)))(Nil)
  }
}


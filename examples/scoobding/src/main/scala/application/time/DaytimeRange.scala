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
package time

import scalaz._
import Scalaz._
import DaytimeRange._
import times._
import java.util._

/**
 * This class represents a start date and an end date, where a date is:
 *
 *  - a CalendarDay
 *  - a time during the day
 *
 *
 *
 */
case class DaytimeRange(start: Daytime = daytime(0L),
                        end: Daytime = DaytimeRange.EOD,
                        startDay: CalendarDay = CalendarDay(),
                        endDay: CalendarDay = CalendarDay()) {

  /** @ return the start time as a string */
  def startTime = showStartDay + toDaytimeDisplay(start).daytime
  /** @ return the start as a Date */
  def startDate = date(startDay, start)
  
  /** @ return the end time as a string */
  def endTime = showEndDay + toDaytimeDisplay(end).daytime
  /** @ return the end as a Date */
  def endDate = date(endDay, end)

  private def date(day: CalendarDay, time: Daytime): Date = day.date(time)

  private def showStartDay = startDay.toString + " "
  private def showEndDay   = endDay.toString   + " "

  /** @ return true if this range spans over 2 days */
  def isOverTwoDays = end < start

  /** @return true if the hour/minute of the epoch parameter is included in this range */
  def contains(other: Epochtime): Boolean = {
    isInfinite ||
    (startDate.getTime <= other  && other <= endDate.getTime)
  }

  /**
   * @return a new DaytimeRange which is extended with a new Epochtime
   */
  def extendWith(time: Epochtime): DaytimeRange = {
    if (isUndefined)                   DaytimeRange(time.toDate, time.toDate)
    else if (startDate.getTime > time) DaytimeRange(time.toDate, endDate)
    else if (time > endDate.getTime)   DaytimeRange(startDate, time.toDate)
    else this
  }

  /** @return true if the time range is undefined */
  def isUndefined = start < 0

  /** @return true if the time range is infinite */
  def isInfinite = this eq INFINITE

  /**
   * if the range is undefined we just choose to show it as the whole day
   */
  override def toString: String =
    if (isInfinite)       "1/1 00:00 -> 31/12 23:59"
    else if (isUndefined) DAY.toString
    else                  (startTime+" -> "+endTime)
}

object DaytimeRange {
  /** description of the accepted formats */
  lazy val TIMERANGE_FORMATS = """Format:
  [dd/MM[/YY[YY]]] hh:mm [-> [dd/MM[/YY[YY]]] hh:mm]

  Examples (today is 16/1/2012):

  INPUT                        => INTERPRETED AS
  14:35                        => 16/1/2012 14:35  -> 16/1/2012 23:59
  28/10 14:35                  => 28/10/2012 14:35 -> 28/10/2012 23:59
  03/09/2013 14:35             => 03/09/2013 14:35 -> 03/09/2013 23:59
  14:35 -> 20:06               => 16/1/2012 14:35  -> 16/1/2012 20:06
  14:35 -> 02:06               => 16/1/2012 14:35  -> 17/1/2012 02:06
  28/10 14:35 -> 20:06         => 28/10/2012 14:35 -> 28/10/2012 20:06
  28/10 14:35 -> 30/10 20:06   => 28/10/2012 14:35 -> 30/10/2012 20:06
  """

  /** DaytimeRange for the full day */
  lazy val DAY = new DaytimeRange()

  /** End of day in milliseconds */
  lazy val EOD = daytime(86399*1000L)

  /** Empty time range. Extending it with any time will make it larger */
  lazy val EMPTY = DaytimeRange(daytime(-1L), daytime(-1L))

  /** Infinite time range, contains any timestamp */
  lazy val INFINITE = DaytimeRange(daytime(Long.MinValue), daytime(Long.MaxValue))


  def apply(start: String, end: String): DaytimeRange = {

    val timeRange = ^(time(start), time(end))(DaytimeRange(_,_)).getOrElse(EMPTY)
    val range =
      (day(start), day(end)) match {
        case (Some(s), Some(e)) => timeRange.copy(startDay = s, endDay = e)
        case (None, Some(e))    => timeRange.copy(startDay = e, endDay = e)
        case (Some(s), None)    => timeRange.copy(startDay = s, endDay = s)
        case (None, None)       => timeRange
      }
    if (range.isOverTwoDays && range.startDay == range.endDay) range.copy(endDay = range.endDay.addDays(1))
    else range

  }

  private def day(s: String): Option[CalendarDay] = {
    val splitted = s.split(" ").filter(_.nonEmpty)
    if (splitted.size == 2) Some(CalendarDay(splitted(0)))
    else                    None
  }

  private def time(s: String): Option[Daytime]  = {
    val splitted = s.split(" ").filter(_.nonEmpty)
    millisFromhhmm(splitted(splitted.size - 1))
  }


  def apply(range: String): DaytimeRange = {
    val splitted = range.split("->").filter(_.nonEmpty)
    if (splitted.size == 2)      apply(splitted(0), splitted(1))
    else if (splitted.size == 1) apply(splitted(0), "23:59")
    else DAY
  }

  def apply(startDate: Date, endDate: Date): DaytimeRange =
    DaytimeRange(epochtime(startDate.getTime).ddMMhhmm,
                 epochtime(endDate.getTime).ddMMhhmm)

}

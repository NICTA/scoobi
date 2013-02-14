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

import java.util.{Date, Calendar}
import Calendar._
import java.text.SimpleDateFormat
import times._
import CalendarDay._
/**
 * This class represents a day in a month, on a given year, like 14/8/2011.
 *
 * An instance of this class can be created from a String like dd/MM
 */
case class CalendarDay(day: Int = todaysDay, month: Int = todaysMonth, year: Int = todaysYear) {

  /** add a number of days to this CalendarDay */
  def addDays(d: Int) ={
    val calendar = getCalendar
    calendar.set(DAY_OF_MONTH, day + d)
    CalendarDay(calendar)
  }

  /** @return the equivalent Date (at 00:00)*/
  def getDate = getCalendar.getTime
  /** @return the equivalent Date (at 00:00)*/
  def getCalendar = setCalendar(startOfDay(Calendar.getInstance))

  /**
   * @return a Date object set with this CalendarDay
   */
  def date(time: Daytime): Date = {
    val calendar = getCalendar
    calendar.setTimeInMillis(time.epochTime)
    setCalendar(calendar)
    calendar.getTime
  }

  /** update the calendar so that it starts at the beginning of the day */
  private def startOfDay(calendar: Calendar) = {
    calendar.set(HOUR_OF_DAY, 0)
    calendar.set(MINUTE, 0)
    calendar.set(SECOND, 0)
    calendar.set(MILLISECOND, 0)
    calendar
  }

  /** update a Calendar object with this CalendarDay */
  private def setCalendar(calendar: Calendar) = {
    calendar.set(DAY_OF_MONTH, day)
    calendar.set(MONTH, month - 1)
    calendar.set(YEAR, year)
    calendar
  }

  override def toString = day+"/"+month+"/"+year
}

object CalendarDay {
  
  private val ddMM = new SimpleDateFormat("dd/MM")
  private val ddMMYY = new SimpleDateFormat("dd/MM/YY")
  private val ddMMYYYY = new SimpleDateFormat("dd/MM/YYYY")

  private[CalendarDay] def todaysDay = Calendar.getInstance().get(DAY_OF_MONTH)
  private[CalendarDay] def todaysMonth = Calendar.getInstance().get(MONTH) + 1
  private[CalendarDay] def todaysYear = Calendar.getInstance().get(YEAR)

  lazy val TODAY = new CalendarDay()

  /**
   * create a CalendarDay from a Date
   */
  def apply(date: Date): CalendarDay = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    CalendarDay(calendar)
  }

  /**
   * create a CalendarDay from a Calendar
   */
  def apply(calendar: Calendar): CalendarDay =
    CalendarDay(calendar.get(DAY_OF_MONTH), 
                calendar.get(MONTH) + 1,
                calendar.get(YEAR))

  /**
   * create a CalendarDay from a dd/MM string
   */
  def apply(s: String): CalendarDay = fromddMM(s)

  /**
   * create a CalendarDay from a dd/MM string
   */
  def fromddMM(s: String): CalendarDay = {
    try {
      val calendar = Calendar.getInstance()
      val date = ddMM.parse(s)
      calendar.setTime(date)
      calendar.set(YEAR, todaysYear)
      CalendarDay(calendar)
    } catch { case e => fromddMMYY(s) }
  }

  /**
   * create a CalendarDay from a dd/MM/YY string
   */
  def fromddMMYY(s: String): CalendarDay = {
    try {
      val calendar = Calendar.getInstance()
      val date = ddMMYY.parse(s)
      calendar.setTime(date)
      CalendarDay(calendar)
    } catch { case e => fromddMMYYYY(s) }
  }

  /**
   * create a CalendarDay from a dd/MM/YYYY string
   */
  def fromddMMYYYY(s: String): CalendarDay = {
    try {
      val calendar = Calendar.getInstance()
      val date = ddMMYYYY.parse(s)
      calendar.setTime(date)
      CalendarDay(calendar)
    } catch { case e => TODAY }
  }

}


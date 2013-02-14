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

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}
import java.util.Calendar._

object times {
  /**
   * This method creates a date format for the local time zone
   */
  private def timeZoneFormat(f: String) = {
    val format = new SimpleDateFormat(f)
    format.setTimeZone(TimeZone.getTimeZone("Australia/Sydney"))
    format
  }

  private lazy val ddMMhhmmFormat   = timeZoneFormat("dd/MM HH:mm")
  private lazy val mmssSFormat   = timeZoneFormat("mm:ss.SSS")
  private lazy val mmssFormat    = timeZoneFormat("mm:ss")
  private lazy val hhmmssSFormat = timeZoneFormat("HH:mm:ss.SSS")

  implicit def toEpochtimeDisplay(t: Epochtime): EpochtimeDisplay = new EpochtimeDisplay(t)
  case class EpochtimeDisplay(t: Epochtime) {
    implicit def toDate: Date = new Date(t)
    lazy val ddMMhhmm = ddMMhhmmFormat.format(t)
    lazy val hhmm = new Date(t).formatted("%1$tH:%1$tM")
    lazy val mmss  = mmssFormat.format(t)
    lazy val mmssS = mmssSFormat.format(t)
    lazy val hhmmssS = hhmmssSFormat.format(t)
  }

  implicit def toDaytimeDisplay(t: Daytime): DaytimeDisplay = new DaytimeDisplay(t)
  case class DaytimeDisplay(t: Daytime) {
    lazy val epoch = new Date(beginningOfDay((new Date).getTime) + t)
    lazy val epochTime = epochtime(epoch.getTime)
    def daytime = epoch.formatted("%1$tH:%1$tM")
    def hhmm = epochTime.hhmm
    def mmss  = epochTime.mmss
    def mmssS = epochTime.mmssS
    def hhmmssS = epochTime.hhmmssS

    /** @return the long representing the beginning of the day for a given time, in the default timezone  */
    private def beginningOfDay(t: Long) = {
      val date = Calendar.getInstance()
      date.setTime(new Date(t))
      date.set(HOUR_OF_DAY, 0)
      date.set(MINUTE, 0)
      date.set(SECOND, 0)
      date.set(MILLISECOND, 0)
      date.getTimeInMillis
    }
  }

  /**
   * @return a time formatted as hh:mm into a long representing the number of millis since the beginning of the day
   */
  def millisFromhhmm(s: String): Option[Daytime] = {
    try {
      val values = s.trim.split("\\:")
      Some(daytime((values(0).trim.toLong*60 + values(1).trim.toLong)*60*1000))
    } catch {
      case e => None
    }
  }

}
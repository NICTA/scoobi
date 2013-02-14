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
import scalazx.Reducer._
import times._

/**
 * Useful trait for anything having a timestamp, like a log record or a measure
 */
trait Timestamped {
  val startTime: Long
  def ddMMhhmm = epochtime(startTime).ddMMhhmm
  def hhmm     = epochtime(startTime).hhmm
  def hhmmssS  = epochtime(startTime).hhmmssS
  def mmssS    = epochtime(startTime).mmssS

  /** @return true if the time range is not defined or if the start time is in the time range */
  def isInTimeRange(timeRange: DaytimeRange) = timeRange.contains(epochtime(startTime))

  // show the record timestamp
  def atTime = " @"+startTime
  def hhmmAtTime = hhmm+atTime
}

object Timestamped {
  /** timestamps define a natural order */
  val startTimeOrder = order((_:Timestamped).startTime)
  /** when the order should not change */
  val noOrder = order((t:Timestamped) => 0L)

}
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
package measure

import time._
/**
 * A Measurement is a set of measures over a given time range
 */
case class Measurement(name: String = "", timeRange: DaytimeRange = DaytimeRange(), measures: Seq[Measure[_]] = Seq()) {

  def startTime = timeRange.startTime
  def endTime = timeRange.endTime

  /** only used for testing */
  def increment(i: Long) = copy(measures = measures map (_.increment(i)))
}




package units

import scala.Predef._
import java.text.SimpleDateFormat
import java.util.{TimeZone, Calendar, Date}
import Calendar._
import time._

/**
 * This trait represents all time units + means of conversion from millis
 */
sealed trait TimeUnit extends MeasureUnit

object Seconds extends TimeUnit {
  val factor = 1000L
  override def toString = "seconds"
}

object Millis extends TimeUnit {
  val factor = 1L
  override def toString = "millis"
}

object EmptyUnit extends MeasureUnit {
  override def toString = ""
  val factor = 1L
}

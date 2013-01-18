import java.text.SimpleDateFormat
import java.util._
import Calendar._

/**
 * This package provides newtypes for times, as Tagged Long types
 */
package object time {

  // Unboxed newtypes, credit to @milessabin and @retronym
  type Tagged[U] = { type Tag = U }
  type @@[T, U] = T with Tagged[U]

  class Tagger[U] {  def apply[T](t : T) : T @@ U = t.asInstanceOf[T @@ U] }
  def tag[U] = new Tagger[U]

  trait Day
  trait Epoch

  // java.lang.Long needs to be used here in order to be used in a case class
  // @see http://issues.scala-lang.org/browse/SI-5183
  type Epochtime = java.lang.Long @@ Epoch
  type Daytime = java.lang.Long @@ Day

  def daytime(i: java.lang.Long): Daytime = tag(i)
  def epochtime(i: java.lang.Long): Epochtime = tag(i)

  /** @return the number of elapsed millis since the beginning of day for that time */
  def epochTimeToDaytime(t: Long): Daytime = {
    val calendar = Calendar.getInstance
    calendar.setTime(new Date(t))
    daytime(((calendar.get(HOUR_OF_DAY)*60+calendar.get(MINUTE))*60+calendar.get(SECOND))*1000 + calendar.get(MILLISECOND))
  }


}


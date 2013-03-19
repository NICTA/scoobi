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


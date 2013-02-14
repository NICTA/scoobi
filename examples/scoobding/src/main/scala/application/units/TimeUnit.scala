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

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




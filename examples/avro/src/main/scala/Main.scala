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
package com.nicta.scoobi.examples

import test.Weather

import com.nicta.scoobi.Scoobi._

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.avro.runtime._

object PageRank extends ScoobiApp {

  // test.Weather is define in src/main/avro/weather.avsc -- 
  // the sbt plugin has code-generated a class for us, so
  // we can easily use it
  def weather(s: String, time: Long, temp: Int) = {
    val w = new Weather
    w.setStation(s)
    w.setTime(time)
    w.setTemp(temp)
    w
  }

  def run() {

    // Let's make a DList of Weather's    
    val w: DList[Weather] = DList(weather("Town Hall", 925, 23), weather("Red Fern", 1201, -55), weather("Bondi Junction", 1920, 100))

    // and we can easily persist it..    
    persist(toAvroFile(w, "test-output", overwrite = true))

    // and of course, it's easy to read it too!

    val w2: DList[Weather] = fromAvroFile[Weather]("test-output")


    // And using the magic of the avro plugin, we can work with case-classes too!

    case class Person(var name: String, var awesomeness: Int)
      extends AvroRecord

    val p = new Person("Eric", 10)

    // and we can do all the cool stuff you're used to

    val joined = DObject(p) join w2

    // and dump it all to a text file, so you can easily verify it worked ;D
    persist(toTextFile(joined, "joined-output", overwrite=true))
  }
}

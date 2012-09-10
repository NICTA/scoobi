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
package com.nicta.scoobi

import org.apache.hadoop.io.{DoubleWritable, BytesWritable, Text}

package object application extends Orderings {

  // see the use of tagged types: http://etorreborre.blogspot.com.au/2011/11/practical-uses-for-unboxed-tagged-types.html
  // this will be replaced with similar functionalities in scalaz 7 when released
  type Tagged[U] = { type Tag = U }
  type @@[T, U] = T with Tagged[U]

  trait AsLevel
  type Level = String @@ AsLevel
  def level(l: String): Level = l.asInstanceOf[Level]

}

trait Orderings {
  implicit def TextOrdering = new Ordering[Text] {
    def compare(x: Text, y: Text): Int = x.compareTo(y)
  }

  implicit def BytesOrdering = new Ordering[BytesWritable] {
    def compare(x: BytesWritable, y: BytesWritable): Int = x.compareTo(y)
  }

  implicit def DoubleOrdering = new Ordering[DoubleWritable] {
    def compare(x: DoubleWritable, y: DoubleWritable): Int = x.compareTo(y)
  }
}
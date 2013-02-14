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

import scalaz.Order
import scalaz.std.string._

/**
 * Type of strings which can be used for keys
 */
case class StringKey(s: String) {
  override def toString = s
}

object StringKey {
  implicit def stringKeyOrder: Order[StringKey] = new Order[StringKey] {
    def order(x: StringKey, y: StringKey) = Order[String].order(x.s, y.s)
  }
}

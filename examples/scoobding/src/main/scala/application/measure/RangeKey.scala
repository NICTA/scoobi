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

import scala.math._
import units.{EmptyUnit, MeasureUnit}
import scalaz.{Order, Semigroup}
import scalaz.Ordering.{GT, LT, EQ}

case class RangeKey(inf: Long, sup: Long) {

  override def toString = inf+" < <= "+sup
  def merge(o: RangeKey) = RangeKey(min(inf, o.inf), max(sup, o.sup))
  def contains(o: RangeKey) = inf <= o.inf && o.sup <= sup

}

object RangeKey {

  def range(value: Long, size: Long, unit: MeasureUnit = EmptyUnit) = {
    val inf = (value / size) * size
    val sup = inf + size
    RangeKey(unit.fromBase(inf), unit.fromBase(sup))
  }


  implicit val rangeKeyIsSemigroup: Semigroup[RangeKey] = new Semigroup[RangeKey] {
    def append(r1: RangeKey, r2: =>RangeKey) = r1.merge(r2)
  }
  implicit def rangeKeyOrder = new Order[RangeKey] {

    def order(x: RangeKey, y: RangeKey) = {
      if (x.contains(y) || y.contains(x)) EQ
      else if (x.sup <= y.inf)            LT
      else                                GT
    }
  }
}


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


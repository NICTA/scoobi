package units

import scalaz._
import Scalaz._
import scalazx.Reducer._
/**
 * This class represent a quantity which is the result of measuring something like an execution time:
 *
 * Quantity("execution time", 1234, Millis)
 *
 * The value must always be stored as the value for the smallest measure unit and the 'unit' field can be used to do the conversion
 *
 */
case class Quantity(name: String, baseValue: Long, unit: MeasureUnit = EmptyUnit) {
  /** @return the value converted to the quantity unit */
  def value = unit.fromBase(baseValue)

  /** @return a displayable representation */
  def show(v: Long) = v+" "+unit

  /** change the unit */
  def withUnit(u: MeasureUnit) = copy(unit = u)

  /** increment with a given quantity, only used for testing */
  def increment(i: Long)  = copy(baseValue = baseValue + i)

  /** divide by a number */
  def divideBy(i: Int)  = copy(baseValue = baseValue / i)
}

object Quantity {

  /** ordering on quantities */
  lazy val quantityOrder : Order[Quantity] = order((_:Quantity).value)

  /** quantities can be added */
  lazy val quantityIsSemigroup: Semigroup[Quantity] = new Semigroup[Quantity] {
    def append(q1: Quantity, q2: =>Quantity) = q1.copy(baseValue = q1.baseValue + q2.baseValue)
  }
}

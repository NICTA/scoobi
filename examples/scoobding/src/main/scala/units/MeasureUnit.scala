package units
/**
 * This trait represents any Unit type
 *
 * It can't be called Unit because this conflicts with the Unit type in scala
 *
 * A MeasureUnit can be converted to another Measure unit assuming that:
 *
 * - there is one MeasureUnit for "base" values, for example Millis
 * - there is a method to convert a "base" value to a "higher" unit, for example Seconds.fromBase converts millis into seconds (toBase is the inverse)
 */
trait MeasureUnit {

  /** a MeasureUnit can be displayed */
  def show: String = toString

  def fromBase = (value: Long) => value / factor
  def toBase   = (value: Long) => value * factor

  /**
   * each unit must define a conversion factor with the unit just below it. For example the factor for Minutes is 60
   */
  def factor: Long

  /** a function creating a value can be lifted to a function creating a quantity, by providing a name for the quantity */
  def createQuantity[T](name: String, f: T => Long): T => Quantity = (t: T) => new Quantity(name, f(t), this)
}

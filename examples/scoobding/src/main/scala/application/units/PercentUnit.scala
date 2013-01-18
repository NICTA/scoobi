package units

/**
 * This measure unit is used when providing a ratio of elements like the percentage of cache utilization
 */
object PercentUnit extends MeasureUnit {

  override def show: String = "%"
  override def toString = ""

  val factor = 1L
}
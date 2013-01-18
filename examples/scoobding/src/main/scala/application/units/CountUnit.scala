package units

/**
 * This measure unit is used when counting elements like the number of database rows read
 */
object CountUnit extends MeasureUnit {
  override def show: String = "number"
  override def toString = ""
  val factor = 1L
}
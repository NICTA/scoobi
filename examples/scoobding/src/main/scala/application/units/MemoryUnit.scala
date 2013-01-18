package units
/**
 * This trait represents all memory units + means of conversion from megabytes
 */
sealed trait MemoryUnit extends MeasureUnit

object Gigabytes extends MemoryUnit {
  val factor = Megabytes.factor * 1000
  override def toString = "Gb"
}

object Megabytes extends MemoryUnit {
  val factor = Kilobytes.factor * 1000
  override def toString = "Mb"
}

object Kilobytes extends MemoryUnit {
  val factor = 1000L
  override def toString = "Kb"
}

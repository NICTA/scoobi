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

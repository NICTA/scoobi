package text

/**
 * utility methods to work on strings
 */
object Text {

  /**
   * This methods provides a default case for a conversion to a number
   */
  implicit def toNumber(s: String) = new ToNumber(s)
  class ToNumber(s: String) {
    def toIntOrZero = try { s.toInt } catch { case e => 0 }
  }

  /** @return an extended String */
    implicit def extendedString(s: String) = new ExtendedString(s)

    class ExtendedString(s: String) {

      /**
       * @return true if s matches an 'include / exclude' regexp:
       *
       *  - if include is defined s must match include
       *  - if exclude is defined s must not match exclude
       *
       *  @see measure.TextSpec
       */
      def matchesOnly(only: String) = try {
        def matches(exp: String) = s matches ".*"+exp+".*"

        val includeExclude = only.trim.split("/").filter(_.nonEmpty)
        if (includeExclude.size == 2)   matches(includeExclude(0).trim) && !matches(includeExclude(1).trim)
          else if (only.trim startsWith "/") !matches(only.trim.drop(1))
          else if (only.trim endsWith "/")   matches(only.trim.dropRight(1))
          else                               matches(only.trim)

      } catch { case _ => true }

    }

}
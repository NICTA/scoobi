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
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
package com.nicta.scoobi
package impl
package text

/**
 * This trait provides functions to transform nouns to
 * their plural form
 *
 */
private[scoobi]
trait Plural {

  /**@return a Noun object which can be pluralised */
  implicit def noun(s: String) = Noun(s)

  case class Noun(s: String) {
    def plural(v: Int) = if (v > 1) s + "s" else s

    def plural(v: Long) = if (v > 1) s + "s" else s

    def bePlural(v: Int) = if (v > 1) s + "are" else s + "is"

    def bePlural(v: Long) = if (v > 1) s + "are" else s + "is"
  }

  /**@return a Quantity which can be applied to a string to pluralise it */
  implicit def quantity(i: Int) = Quantity(i)

  case class Quantity(i: Int) {
    /**@return a pluralised string describing this quantity */
    def qty(s: String) = i.toString + " " + s.plural(i)

    /**
     * @return a Option with a pluralised string describing this quantity if it is
     *         greater than 0
     */
    def optQty(s: String): Option[String] = if (i > 0) Some(qty(s)) else None

    /**
     * @return a Option with a non-pluralised string describing this quantity if it is
     *         greater than 0
     */
    def optInvariantQty(s: String): Option[String] = if (i > 0) Some(i.toString + " " + s) else None
  }

  /**@return an Ordinal which can have a rank in a sequence */
  implicit def ordinal(i: Int) = Ordinal(i)

  case class Ordinal(i: Int) {
    /**
     * @return the proper postfix for an ordinal number
     */
    def th = i.toString +
      (if (i == 1) "st"
      else if (i == 2) "nd"
      else if (i == 3) "rd"
      else "th")
  }

}

private[scoobi]
object Plural extends Plural
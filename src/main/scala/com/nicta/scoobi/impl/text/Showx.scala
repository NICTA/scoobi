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

import scalaz.Show

/**
 * Extension for the Show functionality of Scalaz for sequences where the elements share a same Show instance
 *
 * Note: there are ways to transform this code so that more specific Show instances are picked up for each element in the list
 *       (see StackOverflow and Shapeless)
 */
object Showx {
  def parens[T : Show](t: T) = Seq(t).showString

  implicit def showSeq[T : Show](seq: Seq[T]): ShowSeq[T] = new ShowSeq(seq)
  class ShowSeq[T : Show](seq: Seq[T]) {
    def showString: String = showString()
    def showString(separator: String = ","): String = showString("(", separator, ")")
    def showString(start: String, end: String): String = showString("(", ",", ")")
    def showString(start: String, separator: String, end: String): String = seq.map(implicitly[Show[T]].show).mkString(start, separator, end)
  }
}
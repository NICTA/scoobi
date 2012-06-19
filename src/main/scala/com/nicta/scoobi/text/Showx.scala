package com.nicta.scoobi
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
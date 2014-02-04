package com.nicta
package scoobi
package io
package text

import testing._
import TextOutput._

class TextOutputSpec extends UnitSpecification { def is = s2"""

  For delimited text files we need a toString method working on any kind of Product:
    Tuples      $e1
    case class  $e2
    List        $e3
    Option      $e5
  """

  def e1 = anyToString((1, 2, 3), "|") === "1|2|3"
  def e2 = anyToString(A(1, 2, 3), "|") === "1|2|3"
  def e3 = anyToString(List(1, 2, 3), "|") === "1|2|3"
  def e5 = anyToString(Option(1), "|") === "1"

  case class A(i: Int, j: Int, k: Int)
}



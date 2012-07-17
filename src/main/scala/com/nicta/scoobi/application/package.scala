package com.nicta.scoobi
package object application {

  // see the use of tagged types: http://etorreborre.blogspot.com.au/2011/11/practical-uses-for-unboxed-tagged-types.html
  // this will be replaced with similar functionalities in scalaz 7 when released
  type Tagged[U] = { type Tag = U }
  type @@[T, U] = T with Tagged[U]

  trait AsLevel
  type Level = String @@ AsLevel
  def level(l: String): Level = l.asInstanceOf[Level]
}
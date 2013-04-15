package com.nicta.scoobi
package impl
package types

/**
 * This is a short extract of the Shapeless project: https://github.com/milessabin/shapeless (Apache 2.0 license)
 *
 *
 */
trait TypeOperators {

  // Tags
  trait Tagged[U]
  type @@[+T, U] = T with Tagged[U]

  class Tagger[U] {
    def apply[T](t : T) : T @@ U = t.asInstanceOf[T @@ U]
  }

  def tag[U] = new Tagger[U]

}

object TypeOperators extends TypeOperators

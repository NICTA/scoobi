/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/** Trait that is sub-classed by objects to provide sets of unique identifiers. */
trait UniqueInt {
  var i: Int = 0
  def get: Int = { val ret = i; i = if (i == Int.MaxValue) 0 else i + 1; ret }
}

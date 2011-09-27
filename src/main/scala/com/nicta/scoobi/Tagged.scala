/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/** A heterogenous type. */
abstract class Tagged(private var t: Int) { self =>
  /* Get/set the tag */
  def tag = t
  def setTag(tag: Int) = { self.t = tag }

  /* Get/set the value for a given tag. */
  def get(tag: Int): Any
  def set(tag: Int, x: Any)
}

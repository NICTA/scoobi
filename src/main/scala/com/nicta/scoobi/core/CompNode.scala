package com.nicta.scoobi
package core

import org.kiama.attribution.Attributable

/**
 * Base trait for "computation nodes" with no generic type information for easier rewriting
 */
trait CompNode extends Attributable {
  def id: Int
  def sinks : Seq[Sink]
}


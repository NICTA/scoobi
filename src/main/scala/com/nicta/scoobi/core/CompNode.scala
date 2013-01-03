package com.nicta.scoobi
package core

import org.kiama.attribution.Attributable
import scalaz.Equal

/**
 * Base trait for "computation nodes" with no generic type information for easier rewriting
 */
trait CompNode extends Attributable {
  def id: Int
  def sinks : Seq[Sink]
  def bridgeStore: Option[Bridge]
  def mwf: ManifestWireFormat[_]
  def mf = mwf.mf
  def wf = mwf.wf

  override def equals(a: Any) = a match {
    case n: CompNode => n.id == id
    case other       => false
  }

  override def hashCode = id.hashCode
}

object CompNode {
  implicit def compNodeEqual[T <: CompNode] = new Equal[T] {
    def equal(a1: T, a2: T) = a1.id == a2.id
  }
}


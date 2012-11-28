package com.nicta.scoobi
package impl
package plan
package mscr

import org.specs2.matcher.{Expectable, Matcher}
import org.scalacheck.{Gen, Arbitrary}

import core._
import comp._
import testing.mutable.UnitSpecification
import org.specs2.ScalaCheck

trait MscrAttributes extends ShowNode with CompNodes {
  override def toString = "<MscrAttributes>"
  def mscrAttributes = new MscrAttributes {}
  implicit def mscrAttributesArbitrary: Arbitrary[MscrAttributes] = Arbitrary(Gen.value(mscrAttributes))
}


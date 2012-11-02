package com.nicta.scoobi
package impl
package plan
package mscr

import org.specs2.matcher.{Expectable, Matcher}
import org.scalacheck.{Gen, Arbitrary}

import core._
import comp._
import testing.mutable.UnitSpecification


trait MscrMakerSpecification extends UnitSpecification with CompNodeData {

  def beAnAncestorOf(node: CompNode): Matcher[CompNode] = new Matcher[CompNode] {
    def apply[S <: CompNode](other: Expectable[S]) =
      result(mscrAttributes.isAncestor(node, other.value),
        other.description+" is an ancestor of "+node+": "+(Seq(node) ++ mscrAttributes.ancestors(node)).mkString(" -> "),
        other.description+" is not an ancestor of "+node, other)
  }

  def mscrAttributes = new MscrAttributes {}
  implicit def mscrAttributesArbitrary: Arbitrary[MscrAttributes] = Arbitrary(Gen.value(mscrAttributes))
}

trait MscrAttributes extends MscrMaker with ShowNode with CompNodes {
  def parallelDos(n: CompNode) = (n -> descendents).collect(isAParallelDo)
  override def toString = "<MscrAttributes>"
}


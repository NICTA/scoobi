package com.nicta.scoobi
package impl
package plan
package graph

import org.specs2.matcher.{Expectable, Matcher}
import comp._
import org.scalacheck.{Gen, Arbitrary}
import org.specs2.mutable.Tags
import testing.mutable.UnitSpecification


trait MscrGraphSpecification extends UnitSpecification with CompNodeData with Tags {

  def beAnAncestorOf(node: CompNode): Matcher[CompNode] = new Matcher[CompNode] {
    def apply[S <: CompNode](other: Expectable[S]) =
      result(mscrAttributes.isAncestor(node, other.value),
        other.description+" is an ancestor of "+node+": "+(Seq(node) ++ mscrAttributes.ancestors(node)).mkString(" -> "),
        other.description+" is not an ancestor of "+node, other)
  }

  def mscrAttributes = new MscrAttributes {}
  implicit def mscrAttributesArbitrary: Arbitrary[MscrAttributes] = Arbitrary(Gen.value(mscrAttributes))
}

trait MscrAttributes extends MscrGraph with ShowNode with CompNodes {
  def parallelDos(n: CompNode) = (n -> descendents).collect(isAParallelDo)
  override def toString = "<MscrAttributes>"
}


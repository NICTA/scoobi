package com.nicta.scoobi
package impl
package plan
package graph

import org.specs2.mutable.Specification
import org.specs2.matcher.{Expectable, Matcher}
import comp._
import CompNode._
import org.scalacheck.{Gen, Arbitrary}
import org.specs2.mutable.Tags
import org.specs2.main.CommandLineArguments


class MscrGraphSpec extends MscrGraphSpecification {

  include(arguments,
          new GraphFunctionsSpec,
          new InputChannelsSpec ,
          new OutputChannelsSpec,
          new GbkMscrsSpec      ,
          new OtherMscrsSpec)
}

trait MscrGraphSpecification extends Specification with CompNodeData with Tags {

  def beAnAncestorOf(node: CompNode): Matcher[CompNode] = new Matcher[CompNode] {
    def apply[S <: CompNode](other: Expectable[S]) =
      result(isAncestor(node, other.value),
        other.description+" is an ancestor of "+node+": "+(Seq(node) ++ mscrAttributes.ancestors(node)).mkString(" -> "),
        other.description+" is not an ancestor of "+node, other)
  }

  def mscrAttributes = new MscrAttributes {}
  implicit def mscrAttributesArbitrary: Arbitrary[MscrAttributes] = Arbitrary(Gen.value(mscrAttributes))
}

trait MscrAttributes extends MscrGraph with ShowNode {
  def parallelDos(n: CompNode) = (n -> descendents).collect(isAParallelDo)
  override def toString = "<MscrAttributes>"
}


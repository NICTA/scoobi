package com.nicta.scoobi
package impl
package plan
package mscr

import testing.UnitSpecification
import org.specs2.specification.Groups
import comp.factory
import org.specs2.matcher.ThrownExpectations
import org.specs2.execute.Pending

class   InputChannelSpec extends UnitSpecification with Groups with ThrownExpectations { def is =

  "An InputChannel encapsulates CompNodes which are the inputs of a Mscr"^
    "the inputs of an InputChannel are the input nodes of the channel nodes"! g1().e1^
    "the incomings of an InputChannel are the incoming nodes of the channel nodes"! g1().e2^
    end

  "inputs" - new g1 with factory {
    e1 := Pending("implement with sources") //MapperInputChannel(pd(load, rt)).inputs === Seq(load)
    e2 := Pending("implement with sources") //MapperInputChannel(pd(load, rt)).incomings === Seq(load, rt)
  }
}

package com.nicta.scoobi
package impl
package plan
package mscr

import testing.UnitSpecification
import org.specs2.specification.Groups
import comp.factory

class InputChannelSpec extends UnitSpecification with Groups { def is =

  "An InputChannel encapsulates CompNodes which are the inputs of a Mscr"^
    "the inputs of an InputChannel are the input nodes of the channel nodes"! g1().e1^
  end

  "inputs" - new g1 with factory {
    e1 := MapperInputChannel(pd(load, rt)).inputs === Seq(load, rt)
  }
}

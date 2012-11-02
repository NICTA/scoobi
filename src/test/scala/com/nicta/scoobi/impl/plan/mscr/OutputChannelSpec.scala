package com.nicta.scoobi
package impl
package plan
package mscr

import testing.UnitSpecification
import org.specs2.specification.Grouped
import comp.{StringSink, factory}

class OutputChannelSpec extends UnitSpecification with Grouped { def is =

  "An OutputChannel encapsulates CompNodes which are the outputs of a Mscr"^
    "the sinks of an OutputChannel are"^
      "the sinks of the compnodes if there are some"! g1.e1^
      "a BridgeStore created from one of the nodes otherwise"! g1.e2^
  end

  "sinks" - new g1 with factory {
    e1 := GbkOutputChannel(gbk(load).addSink(StringSink())).sinks === Seq(StringSink())
    e2 := {
      val gbk1 = gbk(load)
      GbkOutputChannel(gbk1).sinks === Seq(gbk1.bridgeStore)
    }
  }
}

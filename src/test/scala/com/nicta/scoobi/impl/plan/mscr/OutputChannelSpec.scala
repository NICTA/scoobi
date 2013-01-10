package com.nicta.scoobi
package impl
package plan
package mscr

import testing.UnitSpecification
import org.specs2.specification.Grouped
import comp.{GroupByKey, StringSink, factory}

class OutputChannelSpec extends UnitSpecification with Grouped { def is =

  "An OutputChannel encapsulates CompNodes which are the outputs of a Mscr"    ^
    "the sinks of an OutputChannel are"                                        ^
    "a BridgeStore created from one of the nodes by default"                   ! g1.e1^
    "the additional sinks of the compnodes if there are some"                  ! g1.e2^
                                                                               end

  "sinks" - new g1 with factory {
    val (gbk1, gbk2) = (gbk(load), gbk(StringSink()))

    e1 := GbkOutputChannel(gbk1).sinks === Seq(gbk1.bridgeStore.get)
    e2 := GbkOutputChannel(gbk2).sinks === Seq(gbk2.bridgeStore.get, StringSink())
  }
}

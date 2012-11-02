package com.nicta.scoobi
package impl
package plan
package comp

import testing.UnitSpecification
import org.specs2.specification.Grouped

class DCompSpec extends UnitSpecification with Grouped { def is =

  "A DComp node represent the current state of a DList or DObject computation"^
  p^
  "Sinks"^
    "it is possible to add a Sink to a node"! g1.e1^
    "it is possible to create a BridgeStore for a node"! g1.e2^
    "it is possible to update all the sinks to add compression for example"! g1.e3^
  end

  "sinks" - new g1 {
    e1 := Return.unit.addSink(StringSink()).sinks === Seq(StringSink())
    e2 := Return.unit.bridgeStore must not(beNull)
    e3 := Return.unit.addSink(StringSink()).updateSinks(s => s.map(_.compress)).sinks.map(_.isCompressed) === Seq(true)
  }

}

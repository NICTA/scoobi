package com.nicta.scoobi
package impl
package plan
package comp

import testing.UnitSpecification
import org.specs2.specification.Grouped
import org.specs2.matcher.ThrownExpectations
import core.WireFormat._
import Scoobi._
import mapreducer.BridgeStore
import TextOutput._

class ProcessNodeSpec extends UnitSpecification with Grouped with ThrownExpectations { def is =

  "A ProcessNode node represent the current state of a DList or DObject computation"                 ^
                                                                                                     p^
  "Sinks can be added to ParallelDo, GroupByKey and Combine nodes"                                   ^
    "it is possible to add a Sink to a node"                                                         ! g1.e1^
    "it is possible to create a BridgeStore for a node"                                              ! g1.e2^
    "it is possible to update all the sinks to add compression for example"                          ! g1.e3^
    "if the only available sink is a TextFile then no bridgeStore must be available"                 ! g1.e4^
    "if there is already a BridgeStore in the available sinks, it must be taken as the BridgeStore"+
    " for the node"                                                                                  ! g1.e5^
                                                                                                     end

  "sinks" - new g1 {
    def parallelDo = ParallelDo.create(Return.unit)(wireFormat[String])

    e1 := parallelDo.addSink(StringSink()).nodeSinks === Seq(StringSink())
    e2 := parallelDo.bridgeStore must beSome
    e3 := parallelDo.addSink(StringSink()).updateSinks(s => s.map(_.compress)).nodeSinks.map(_.isCompressed) === Seq(true)
    e4 := parallelDo.addSink(textFileSink("path")).bridgeStore must beNone
    e5 := {
      val bridgeStore = BridgeStore("123", parallelDo.wf)
      parallelDo.addSink(bridgeStore).bridgeStore === Some(bridgeStore)
      parallelDo.addSink(bridgeStore).addSink(textFileSink("path")).bridgeStore === Some(bridgeStore)
    }
  }

}

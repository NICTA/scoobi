/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

class ProcessNodeSpec extends UnitSpecification with Grouped with ThrownExpectations { def is = s2"""

  A ProcessNode node represent the current state of a DList or DObject computation                 
                                                                                                     
  Sinks can be added to ParallelDo, GroupByKey and Combine nodes                                   
    it is possible to add a Sink to a node                                                           ${g1.e1}
    it is possible to update all the sinks to add compression for example                            ${g1.e2}
    if the only available sink is a TextFile there must a distinct bridgeStore                       ${g1.e3}
    if there is already a Bridge in the sinks, it must be taken as the BridgeStore                   ${g1.e4}
                                                                                                     """

  "sinks" - new g1 {
    def parallelDo = ParallelDo.create(Return.unit)(wireFormat[String])

    e1 := parallelDo.addSink(StringSink()).nodeSinks === Seq(StringSink())
    e2 := parallelDo.addSink(StringSink()).updateSinks(s => s.map(_.compress)).nodeSinks.map(_.isCompressed) === Seq(true)
    e3 := {
      val sink = textFileSink("path")
      parallelDo.addSink(sink).bridgeStore must be_!=(sink)
    }
    e4 := {
      val bridgeStore = BridgeStore("123", parallelDo.wf)
      parallelDo.addSink(bridgeStore).bridgeStore === bridgeStore
    }
  }

}

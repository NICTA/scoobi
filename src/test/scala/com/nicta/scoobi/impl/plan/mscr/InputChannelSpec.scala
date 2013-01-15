package com.nicta.scoobi
package impl
package plan
package mscr

import testing.UnitSpecification
import org.specs2.specification.Groups
import comp.{GroupByKey, factory}
import org.specs2.matcher.ThrownExpectations
import core.CompNode
import core.WireFormat._

class InputChannelSpec extends UnitSpecification with Groups with ThrownExpectations { def is =

  """
  An InputChannel encapsulates parallelDo nodes which have a common source node.
  There are 2 types of InputChannels:

   * Mapper: which connects to Gbks
   * Floating: which connects to other types of Nodes
  """^
                                                                                                                        p^
  "2 input channels with 2 different source nodes must be different"                                                    ! g1().e1^
  "the source of the input channel is the Load source if the sourceNode is a Load"                                      ! g1().e2^
  "the source of the input channel is the BridgeStore source if the sourceNode is a ProcessNode"                        ! g1().e3^
  "the input nodes of the input channel are: the source node+the environments of all the channel mappers"               ! g1().e4^
  "an input channel defines tags which are all the gbks ids it relates to"                                              ! g1().e5^
  "an input channel defines the types of keys emitted by tag"                                                           ^
    "as the types of the gbks key for a GbkInputChannel"                                                                ! g1().e6^
    "as Int for a FloatingInputChannel"                                                                                 ! g1().e7^p^
  "an input channel defines the types of values emitted by tag"                                                         ^
    "as the types of the gbks values for a GbkInputChannel"                                                             ! g1().e8^
    "as the types of the last mappers for a FloatingInputChannel"                                                       ! g1().e9^
                                                                                                                        end

  "general properties of input channels" - new g1 with factory {
    val (l1, l2) = (load, load)
    val pd1 = pd(l1)
    val (pd2, gbk1) = (pd(pd1), gbk(pd1))
    val gbk2 = gbk(pd2)

    e1 := {
      gbkInputChannel(l1)  === gbkInputChannel(l1)
      gbkInputChannel(l1) !=== gbkInputChannel(l2)
    }

    e2 := { gbkInputChannel(l1).source === l1.source }
    e3 := { gbkInputChannel(gbk1).source === gbk1.bridgeStore }
    e4 := { gbkInputChannel(l1).inputNodes === Seq(l1, pd1.env, pd2.env) }
    e5 := { gbkInputChannel(l1, Seq(gbk1, gbk2)).tags === Seq(gbk1.id, gbk2.id) }
    e6 := {
      gbkInputChannel(l1, Seq(gbk1, gbk2)).keyTypes.tags === Seq(gbk1.id, gbk2.id)
      gbkInputChannel(l1, Seq(gbk1, gbk2)).keyTypes.types.values.toList === Seq((gbk1.wfk, gbk1.gpk), (gbk2.wfk, gbk2.gpk))
    }
    e7 := {
      floatingInputChannel(l1).keyTypes.tags === Seq(pd2.id)
      floatingInputChannel(l1).keyTypes.types.values.toList.mkString(",") === "(Int,AllGrouping)"
    }
    e8 := {
      gbkInputChannel(l1, Seq(gbk1, gbk2)).valueTypes.tags === Seq(gbk1.id, gbk2.id)
      gbkInputChannel(l1, Seq(gbk1, gbk2)).valueTypes.types.values.toList === Seq(Tuple1(gbk1.wfv), Tuple1(gbk2.wfv))
    }
    e9 := {
      floatingInputChannel(l1).valueTypes.tags === Seq(pd2.id)
      floatingInputChannel(l1).valueTypes.types.values.toList === Seq(Tuple1(pd2.wf))
    }

    def gbkInputChannel(sourceNode: CompNode, groupByKeys: Seq[GroupByKey] = Seq()) = new GbkInputChannel(sourceNode, groupByKeys)
    def floatingInputChannel(sourceNode: CompNode) = new FloatingInputChannel(sourceNode)
  }
}

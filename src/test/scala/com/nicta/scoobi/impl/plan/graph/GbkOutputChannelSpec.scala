package com.nicta.scoobi
package impl
package plan
package graph

import testing.mutable.UnitSpecification
import comp.CompNodeFactory

class GbkOutputChannelSpec extends UnitSpecification with CompNodeFactory {
  val (gbk1, flatten1, cb1, pd1) = (gbk(load), flatten(load), cb(load), pd(load))

  "the output node of a GbkOutputChannel is the flatten node if there is one" >> {
    GbkOutputChannel(gbk1, flatten = Some(flatten1)).output === flatten1
  }
  "it is a combiner node otherwise" >> {
    GbkOutputChannel(gbk1, flatten = Some(flatten1), combiner = Some(cb1)).output === cb1
  }
  "it is a reducer node otherwise" >> {
    GbkOutputChannel(gbk1, flatten = Some(flatten1), combiner = Some(cb1), reducer = Some(pd1)).output === pd1
  }
  "it is the Gbk node otherwise" >> {
    GbkOutputChannel(gbk1).output === gbk1
  }

  ""
}

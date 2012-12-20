package com.nicta.scoobi
package impl
package plan
package mscr

import comp.CompNodeFactory
import testing.mutable.UnitSpecification

class GbkOutputChannelSpec extends UnitSpecification with CompNodeFactory {
  val (gbk1, cb1, pd1) = (gbk(load), cb(load), pd(load))

  "the output node of a GbkOutputChannel is the combiner node if there is one" >> {
    GbkOutputChannel(gbk1, combiner = Some(cb1)).output === cb1
  }
  "it is a reducer node otherwise" >> {
    GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(pd1)).output === pd1
  }
  "it is the Gbk node otherwise" >> {
    GbkOutputChannel(gbk1).output === gbk1
  }

}

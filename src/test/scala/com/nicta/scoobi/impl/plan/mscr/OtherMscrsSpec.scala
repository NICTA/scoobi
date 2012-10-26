package com.nicta.scoobi
package impl
package plan
package mscr

import Mscr._
import collection.IdSet

class OtherMscrsSpec extends MscrMakerSpecification {
  "We must build MSCRs for 'floating' nodes, i.e. not related to Gbk mscrs".txt

  "ParallelDo mscrs" >> {
    "There should be 1 Mscr per floating ParallelDo with a MapperInputChannel and a BypassOutputChannel" >> new factory {
      val l1 = load
      val pd1 = pd(l1)
      val pd2 = pd(l1)
      val op1 = op(pd1, pd2)

      makeMscrs(op1).filter(isParallelDoMscr).toSeq(0) ==== Mscr(inputChannels  = Set(MapperInputChannel(IdSet(pd1, pd2))),
                                                                       outputChannels = Set(BypassOutputChannel(pd1), BypassOutputChannel(pd2)))

    }
  }
  "Flatten mscrs" >> {
    "There should be 1 Mscr per floating Flatten with one input channel for each input of the Flatten and a FlattenOutputChannel" >> new factory {
      val l1 = load
      val l2 = load
      val pd1 = pd(l1)
      val pd2 = pd(l1)
      val fl1 = flatten(pd1, pd2, l2)

      makeMscrs(fl1).filter(isFlattenMscr).toSeq(0) ==== Mscr(inputChannels = Set(StraightInputChannel(l2),
                                                                                    MapperInputChannel(IdSet(pd1)),
                                                                                    MapperInputChannel(IdSet(pd2))),
                                                              outputChannels = Set(FlattenOutputChannel(fl1)))
    }
  }

}
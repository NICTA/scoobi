package com.nicta.scoobi
package impl
package plan
package mscr

import comp.{StringSink, factory}

class OutputChannelsSpec extends MscrMakerSpecification {

  "We must create OutputChannels for each Mscr".newp

  "GbkOutputChannels" >> {
    "There should be 1 GbkOutputChannel for 1 GroupByKey" >> new factory {
      val gbk1 = gbk(cb(load))
      makeMscr(gbk1).outputChannels.toSeq ==== Seq(GbkOutputChannel(gbk1))
    }
    "There should be one GbkOutputChannel for each GroupByKey in the Mscr" >> new factory {
      val l1 = load
      val gbk1 = gbk(cb(l1))
      val gbk2 = gbk(cb(l1))
      val fl = flatten(gbk1, gbk2)
      makeMscrs(fl).flatMap(_.outputChannels) must contain(GbkOutputChannel(gbk1), GbkOutputChannel(gbk2))
    }
    "If the input of a GroupByKey is a Flatten node then add it to this channel" >> new factory {
      val fl1  = flatten(load)
      val gbk1 = gbk(fl1)
      makeMscr(gbk1).outputChannels.toSeq ==== Seq(GbkOutputChannel(gbk1, flatten = Some(fl1)))
    }
    "If the output of a GroupByKey is a Combine node then add it to this channel" >> new factory {
      val gbk1 = gbk(pd(load))
      val cb1  = cb(gbk1)
      makeMscr(gbk1).combiners ==== Set(cb1)
    }
    "If the Combine following a GroupByKey is followed by a ParallelDo, then the ParallelDo can be added as a reducer" >> {
      "if it has a groupBarrier" >> new factory {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1, groupBarrier = true)
        makeMscr(gbk1).reducers ==== Set(pd1)
      }
      "if it has a fuseBarrier" >> new factory {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1, fuseBarrier = true)
        makeMscr(gbk1).reducers ==== Set(pd1)
      }
      "if it has no successor" >> new factory {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1)
        makeMscr(gbk1).reducers ==== Set(pd1)
      }
      "if it has a Materialize successor" >> new factory {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1)
        val mat1 = mt(pd1)
        makeMscr(gbk1).reducers ==== Set(pd1)
      }
      "if it has a no ancestors" >> new factory {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1 = pd(cb1, groupBarrier = false, fuseBarrier = false)
        makeMscr(gbk1).reducers ==== Set(pd1)
      }
      "but it's not added if none of those conditions is true" >> new factory {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val gbk2  = gbk(pd(cb1, groupBarrier = false, fuseBarrier = false))
        val m = makeMscr(gbk2)
        m.reducers aka show(gbk2) must beEmpty
      }
    }
  }
  "BypassOutputChannels" >> {
    "There must be a BypassOutputChannel for each ParallelDo input having outputs which are not gbks" >> new factory {
      val l1 = load
      val pd1 = pd(l1)
      val gbk1 = gbk(pd1)
      val fl1 = flatten(gbk1, pd1)
      makeMscrs(fl1).flatMap(_.bypassOutputChannels) aka mscrsGraph(fl1) must_== Set(BypassOutputChannel(pd1))
    }
  }
  "GbkOutputChannels + BypassOutputChannels" >> {
    "The output channels of a node are all the gbk output channels for that node + the bypass output channels" >> new factory {
      val (l1, l2, l3, rt1, rt2, rt3) = (load, load, load, rt, rt, rt)
      val (pd1, pd2, pd3, pd4) = (pd(l1, rt1), pd(l1, rt2), pd(l2, rt2), pd(l3, rt3))
      val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
      val graph = flatten(gbk1, gbk2, pd3)
      val (mscr1, mscr2, mscr3) = (makeMscr(gbk1), makeMscr(graph), mscr(pd4))
      val mscrs = Seq(mscr1, mscr2)

      "the channels have been properly created:" + mscrs.mkString("\n", "\n", "") <==> {
        mscr1.gbkOutputChannels     must have size(2)
        mscr1.bypassOutputChannels  must have size(0)
        mscr1.flattenOutputChannels must have size(0)
        mscr1.outputChannels        must have size(2)

        mscr2.gbkOutputChannels     must have size(0)
        mscr2.bypassOutputChannels  must have size(0)
        mscr2.flattenOutputChannels must have size(1)
        mscr2.outputChannels        must have size(1)

        mscr3.gbkOutputChannels     must have size(0)
        mscr3.bypassOutputChannels  must have size(1)
        mscr3.flattenOutputChannels must have size(0)
        mscr3.outputChannels        must have size(1)
      }
    }
  }
  "Output channels must have datasinks" >> {
    "they must be the datasinks related to output nodes at the end of the graph" >> new factory {
      val graph = gbk(pd(load)).addSink(StringSink())
      val m = makeMscr(graph)

      m.gbkOutputChannels.head.sinks.map(_.getClass.getSimpleName) === Seq("StringSink")
    }
    "they must be the BridgeStores if output nodes are the input of another Mscr" >> new factory {
      val graph1 = gbk(pd(load)).addSink(StringSink())
      val graph2 = gbk(pd(graph1))
      val mscrs = makeMscrs(graph2)

      mscrs.flatMap(_.outputChannels.flatMap(_.sinks)).map(_.getClass.getSimpleName).toList === Seq("BridgeStore", "StringSink")

    }
  }
}


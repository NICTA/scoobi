package com.nicta.scoobi
package impl
package exec

import org.specs2.mutable.Specification
import com.nicta.scoobi.impl.plan.graph.factory
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class OutputChannelExecSpec extends Specification {
  "Output channels must configure the MapReduceJob".txt
  
  "A GroupByKeyChannelExec" >> {
//          oc match {
//        case GbkOutputChannel(_, _, _, JustCombiner(c))          => job.addTaggedCombiner(c.mkTaggedCombiner(tag))
//        case GbkOutputChannel(_, _, _, CombinerReducer(c, _, _)) => job.addTaggedCombiner(c.mkTaggedCombiner(tag))
//        case _                                                   => Unit
//      }

    "must not configure a Combiner if the channel has no combiner node" >> new example {
      GbkOutputChannelExec(gbkExec).configure(job).combiners must be empty
    }
    "must configure a Combiner if the channel has a combiner node" >> new example {
      GbkOutputChannelExec(gbkExec, combiner = Some(gbkCombinerExec)).configure(job).combiners must have size(1)
    }
    "must configure a Reducer if the channel has a reducer node" >> new example {
      GbkOutputChannelExec(gbkExec, reducer = Some(gbkReducerExec)).configure(job).reducers must have size(1)
    }
  }

  trait example extends execfactory {
    val job = new MapReduceJob(0)
  }
}


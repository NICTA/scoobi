package com.nicta.scoobi
package impl
package exec

import org.specs2.mutable.Specification
import com.nicta.scoobi.impl.plan.graph.factory
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import application.ScoobiConfiguration

@RunWith(classOf[JUnitRunner])
class OutputChannelExecSpec extends Specification {
  implicit val sc = ScoobiConfiguration()

  "Output channels must configure the MapReduceJob".txt

  "A GroupByKeyChannelExec" >> {
    implicit val sc = ScoobiConfiguration()

    "must not configure a Combiner if the channel has no combiner node" >> new example {
      GbkOutputChannelExec(gbkExec).configure(job).combiners must be empty
    }
    "must configure a Combiner if the channel has a combiner node" >> new example {
      GbkOutputChannelExec(gbkExec, combiner = Some(gbkCombinerExec)).configure(job).combiners must have size(1)
    }

    "must configure a Reducer if the channel has no Combiner or no Reducer nodes" >> new example {
      GbkOutputChannelExec(gbkExec).configure(job).reducers must have size(1)
    }
    "must configure a Reducer if the channel has a Combiner node" >> new example {
      GbkOutputChannelExec(gbkExec, combiner = Some(gbkCombinerExec)).configure(job).reducers must have size(1)
    }
    "must configure a Reducer if the channel has a reducer node" >> new example {
      GbkOutputChannelExec(gbkExec, reducer = Some(gbkReducerExec)).configure(job).reducers must have size(1)
    }
    "must configure a Reducer if the channel has a combiner and a reducer node, created from the reducer" >> new example {
      GbkOutputChannelExec(gbkExec, combiner = Some(gbkCombinerExec), reducer = Some(gbkReducerExec)).configure(job).reducers must have size(1)
    }
  }

  "A BypassOutputChannelExec" >> {
    "must configure a Reducer" >> new example {
      BypassOutputChannelExec(pdExec).configure(job).reducers must have size(1)
    }
  }
  "A FlattenOutputChannelExec" >> {
    "must configure a Reducer" >> new example {
      FlattenOutputChannelExec(flattenExec).configure(job).reducers must have size(1)
    }
  }

  trait example extends execfactory {
    val job = new MapReduceJob(0)
  }
}


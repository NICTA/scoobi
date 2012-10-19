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
  "Output channels must configure the MapReduceJob".txt
  
  "A GroupByKeyChannelExec" >> {
    implicit val sc = ScoobiConfiguration()

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


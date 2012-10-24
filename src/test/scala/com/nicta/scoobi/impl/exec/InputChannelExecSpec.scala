package com.nicta.scoobi
package impl
package exec

import testing.mutable.UnitSpecification
import application.ScoobiConfiguration

class InputChannelExecSpec extends UnitSpecification {
  
  "A MapperInputChannelExec has a data source which must be the source of one parallel do in the channel" >> new example {
    (new MapperInputChannelExec(Seq(pdExec)).source: Any) === load.source
  }
  "A BypassInputChannelExec has a data source which must be the source of the input node of the channel" >> new example {
    (new BypassInputChannelExec(pdExec, gbkExec).source: Any) === load.source
  }
  "A StraightInputChannelExec has a data source which must be the source of the input node of the channel" >> new example {
    (new StraightInputChannelExec(flattenPdExec).source: Any) must beLike { case BridgeStore() => ok }
  }
  
  "Input channels must configure the MapReduceJob".txt

  implicit val sc = ScoobiConfiguration()

  "A MapperInputChannelExec must configure a Mapper" >> new example {
    new MapperInputChannelExec(Seq(pdExec)).configure(job).mappers must have size(1)
  }
  "A BypassInputChannelExec must configure a Mapper" >> new example {
    new BypassInputChannelExec(pdExec, gbkExec).configure(job).mappers must have size(1)
  }
  "A StraightInputChannelExec must configure a Mapper" >> new example {
    new StraightInputChannelExec(flattenExec).configure(job).mappers must have size(1)
  }

  trait example extends execfactory {
    val job = new MapReduceJob(0)
  }
}

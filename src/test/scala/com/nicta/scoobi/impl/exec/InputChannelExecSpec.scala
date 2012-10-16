package com.nicta.scoobi
package impl
package exec

import testing.mutable.UnitSpecification

class InputChannelExecSpec extends UnitSpecification with CompNodeExecFactory {
  "A MapperInputChannelExec has a data source which must be the source of one parallel do in the channel" >> {
    (new MapperInputChannelExec(Seq(pdExec)).source: Any) === load.source
  }
  "A BypassInputChannelExec has a data source which must be the source of the input node of the channel" >> {
    (new BypassInputChannelExec(pdExec).source: Any) === load.source
  }
  "A StraightInputChannelExec has a data source which must be the source of the input node of the channel" >> {
    (new StraightInputChannelExec(flattenExec).source: Any) must beLike { case BridgeStore() => ok }
  }
}

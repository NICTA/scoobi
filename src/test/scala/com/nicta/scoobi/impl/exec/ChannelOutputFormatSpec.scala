package com.nicta.scoobi
package impl
package exec

import ChannelOutputFormat._
import org.apache.hadoop.fs.Path
import testing.mutable.UnitSpecification

class ChannelOutputFormatSpec extends UnitSpecification {
  "Channels determine result files for a given job run" >> {
    "_SUCCESS is a result file for any sink" >> {
      isResultFile(0, 0)(new Path("_SUCCESS"))
    }
    "ch1out2-r-00000 is a result file for a sink with tag 1 and index 2" >> {
      isResultFile(1, 2)(new Path("ch1out2-r-00000"))
    }
  }

}

package com.nicta.scoobi
package impl
package mapreducer

import org.apache.hadoop.fs.Path

import ChannelOutputFormat._
import testing.mutable.UnitSpecification

class ChannelOutputFormatSpec extends UnitSpecification {
  "Channels determine result files for a given job run" >> {
    "ch1out2-r-00000 is a result file for a sink with tag 1 and sink id 2" >> {
      isResultFile(1, 2)(new Path("ch1out2-r-00000"))
    }
  }

}

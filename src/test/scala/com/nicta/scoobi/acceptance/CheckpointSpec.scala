package com.nicta.scoobi
package acceptance

import testing.mutable.NictaSimpleJobs
import testing.{TestFiles, TempFiles}
import Scoobi._
import impl.plan.comp.CompNodeData._
import TestFiles._
import CheckpointEvaluations1._

class CheckpointSpec  extends NictaSimpleJobs with ResultFiles {

  "1. It is possible to add checkpoint files which are reused on a subsequent run" >> { implicit sc: SC =>
    val sink = TempFiles.createTempDir("test")

    // do some computations but store the intermediate result and reuse it if available
    evaluationsNb1 = 0
    def compute = {
      val list = DList(1, 2, 3).map { i =>
        if (i == 1) { evaluationsNb1 += 1 }
        "i"+i
      }.toAvroFile(path(sink)).checkpoint("name")
      val list2 = list.map(_ + "1")
      list2.run
    }

    // compute once
    normalise(compute) === "Vector(i11, i21, i31)"

    // compute twice
    normalise(compute) === "Vector(i11, i21, i31)"
    "the intermediate results must be used instead of recomputing the list" ==> {
      // this way of testing if the computation has been done only once can only work locally
      evaluationsNb1 must be_==(1).unless(sc.isRemote)
    }
  }

  "2. If no data is available at the checkpoint, then proceed as usual" >> { implicit sc: SC =>
    pending
  }
}

object CheckpointEvaluations1 {
  var evaluationsNb1: Int = 0
}


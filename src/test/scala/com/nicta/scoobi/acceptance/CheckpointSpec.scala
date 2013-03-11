package com.nicta.scoobi
package acceptance

import testing.mutable.NictaSimpleJobs
import testing.{TestFiles, TempFiles}
import Scoobi._
import impl.plan.comp.CompNodeData._
import testing.TestFiles._
import CheckpointEvaluations1._
import java.io.File

class CheckpointSpec extends NictaSimpleJobs with ResultFiles { sequential

  "1. It is possible to add checkpoint files which are reused on a subsequent run" >> { implicit sc: SC =>
    checkEvaluations()
  }

  "2. A checkpoint must be used when the program is restarted" >> { implicit sc: SC =>
    checkEvaluations(restart = true)
  }

  "3. A checkpoint must work after a group by key" >> { implicit sc: SC =>
    val sink = TempFiles.createTempDir("test")
    val list = DList(1, 2, 3).map(i => (i.toString, i + 1)).toAvroFile(path(sink)(configuration), overwrite = true).checkpoint
    list.groupByKey.combine(Sum.int).run.normalise === "Vector((1,2), (2,3), (3,4))"
  }

  def checkEvaluations(restart: Boolean = false)(implicit sc: SC) = {
    val sink = TempFiles.createTempDir("test")

    // do some computations but store the intermediate result and reuse it if available
    evaluationsNb1 = 0

    // compute once
    normalise(compute(sink)) === "Vector(i11, i21, i31)"

    // compute twice
    normalise(compute(sink, restart)) === "Vector(i11, i21, i31)"
    "the intermediate results must be used instead of recomputing the list" ==> {
      // this way of testing if the computation has been done only once can only work locally
      evaluationsNb1 must be_==(1).unless(sc.isRemote)
    }
  }

  def compute(sink: File, restart: Boolean = false)(implicit sc: ScoobiConfiguration) = {
    // restart the configuration if necessary
    val configuration = if (restart) sc.reset else sc

    val list = DList(1, 2, 3).map { i =>
      if (i == 1) { evaluationsNb1 += 1 }
      "i"+i
    }.toAvroFile(path(sink)(configuration), overwrite = true).checkpoint

    val list2 = list.map(_ + "1")
    list2.run(configuration)
  }
}

object CheckpointEvaluations1 { var evaluationsNb1: Int = 0 }


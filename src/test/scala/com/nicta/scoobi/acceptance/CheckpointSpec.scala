/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package acceptance

import testing.mutable.NictaSimpleJobs
import testing.{TestFiles, TempFiles}
import Scoobi._
import impl.plan.comp.CompNodeData._
import com.nicta.scoobi.testing.TestFiles._
import CheckpointEvaluations1._
import java.io.File
import com.nicta.scoobi.core.ExpiryPolicy
import org.apache.hadoop.fs.Path
import com.nicta.scoobi.application.Application.ScoobiConfiguration
import scala.concurrent.duration._
import org.specs2.time.NoTimeConversions

class CheckpointSpec extends NictaSimpleJobs with ResultFiles with NoTimeConversions { sequential

  "1. It is possible to add checkpoint files which are reused on a subsequent run" >> { implicit sc: SC =>
    checkEvaluations()
  }

  "2. A checkpoint must be used when the program is restarted" >> { implicit sc: SC =>
    checkEvaluations(restart = true)
  }

  "3. A checkpoint must work after a group by key" >> { implicit sc: SC =>
    val sink = TempFiles.createTempDir("test")
    val list = DList(1, 2, 3).map(i => (i.toString, i + 1)).toAvroFile(path(sink)(configuration), overwrite = true, checkpoint = true)
    list.groupByKey.combine(Reduction.Sum.int).run.normalise === "Vector((1,2), (2,3), (3,4))"
  }

  "4. A checkpoint can be created with a path, as a BridgeStore" >> { implicit sc: SC =>
    evaluationsNb2 = 0
    val sink = TempFiles.createTempDir("test")
    val list = DList(1, 2, 3).map(i => { if (i == 1) evaluationsNb2 += 1; i + 1 }).checkpoint(path(sink))

    list.run.normalise === "Vector(2, 3, 4)"
    list.run.normalise === "Vector(2, 3, 4)"

    "the intermediate results must be used instead of recomputing the list" ==> {
      // this way of testing if the computation has been done only once can only work locally
      evaluationsNb2 must be_==(1).unless(sc.isRemote)
    }

    "the checkpoint files must be written at the right place" ==> { sink.listFiles must not be empty }
  }

  "5. there must be an expiry date on checkpoint files" >> {
    "If the expiry date is passed the expiry policy must be used to archive files" >> {
      "By default the old files are suppressed" >> { implicit sc: SC =>
        val archiving = (p: Path, sc: ScoobiConfiguration) => {
          oldFileIsDeleted = true
          ExpiryPolicy.deleteOldFile(p, sc)
        }
        runListWithExpiry(archiving)
        "the expired files have been suppressed" ==> (oldFileIsDeleted must beTrue).unless(sc.isRemote)
      }
      "But the old output directory can also be renamed" >> { implicit sc: SC =>
        val sink = runListWithExpiry(ExpiryPolicy.incrementCounterFile)
        "the expired files have been renamed" ==> {
          (sink.getParentFile.listFiles.map(_.getName).toSeq must contain((s: String) => s === sink.getName+"-1")).unless(sc.isRemote)
        }
      }
    }
  }

  "6. If there are 2 checkpointed Hadoop jobs in the same Scoobi job and one of them fails, the checkpoint of the first one "+
   "must be used" >> { implicit sc: SC =>
    val (sink1, sink2) = (TempFiles.createTempDir("test1"), TempFiles.createTempDir("test2"))

    val list0 = DList(1, 2, 3)
    val list1 = list0.map { i =>
      if (i == 1) { example6_evaluations += 1 }
      i + 1
    }.checkpoint(path(sink1)).map(_+1)

    val list2 = list0.map(i => (i, i)).groupByKey.map { case (k, vs) => (k, k) }.groupByKey.map { case (k, vs) =>
      if (example6_list2_first_try) {
        example6_list2_first_try = false
        sys.error("fails the first time")
      }
      k
    }.checkpoint(path(sink2)).map(_+1)

    // compute once, ignore the exception
    try { persist(list1, list2) } catch { case e: Exception => }

    // compute twice
    persist(list1, list2)

    "the intermediate results are used for job1 instead of recomputing the list" ==> {
      example6_evaluations must be_==(1)
    }
  }


  def runListWithExpiry(archiving: ExpiryPolicy.ArchivingPolicy)(implicit sc: SC): File = {
    val sink = TestFiles.createTempDir("test")
    persist(list(sink.getPath, 1 second, archiving))
    Thread.sleep(1000*3)
    persist(list(sink.getPath, 1 second, archiving))
    sink
  }

  def list(sink: String, expiry: FiniteDuration, archiving: ExpiryPolicy.ArchivingPolicy)(implicit sc: SC) =
    DList(1, 2, 3).map(_+1).checkpoint(path(sink), expiryPolicy = ExpiryPolicy(expiryTime = expiry, archive = archiving))
      .map(_+1)

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
      // this is why the specification is marked as isCluster = false
      evaluationsNb1 must be_==(1)
    }
  }

  def compute(sink: File, restart: Boolean = false)(implicit sc: ScoobiConfiguration) = {
    // restart the configuration if necessary
    val configuration = if (restart) sc.duplicate else sc

    val list = DList(1, 2, 3).map { i =>
      if (i == 1) { evaluationsNb1 += 1 }
      "i"+i
    }.toAvroFile(path(sink)(configuration), overwrite = true, checkpoint = true)

    val list2 = list.map(_ + "1")
    list2.run(configuration)
  }

  override def isInMemory = false
  override def isCluster = false
}

object CheckpointEvaluations1 {
  var evaluationsNb1: Int = 0
  var evaluationsNb2: Int = 0
  
  var example6_evaluations = 0
  var example6_list2_first_try = true
  
  var oldFileIsDeleted = false
}


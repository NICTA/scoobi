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
package core

import testing.{InputStringTestFile, TestFiles}
import testing.mutable.NictaSimpleJobs
import Scoobi._
import impl.plan.comp.CompNodeData
import CompNodeData._
import org.specs2.matcher.TerminationMatchers

class DObjectSpec extends NictaSimpleJobs with TerminationMatchers {

  tag("issue 113")
  "it must be possible to take the minimum and the maximum of a list" >> { implicit sc: SC =>
    val r = DList(1, 2, 3, 4)
    val min = persist(r.min)
    val max = persist(r.max)
    (min, max) === (1, 4)
  }

  tag("issue 156")
  "it must be possible to turn a DObject to a DList" >> { implicit sc: SC =>
    DObject(5).toSingleElementDList.run === Seq(5)
    DObject(Seq(10, 7, 5)).toDList.run === Seq(10, 7, 5)
  }

  tag("issue 210")
  "zipping with itself" >> { implicit sc: SC =>
    val input = InputStringTestFile(Seq("a", "b", "c")).lines
    (input.size zip input.size).run.normalise === "(3,3)"
  }

  tag("issue 256")
  "A DObject can be created from a sequence of elements which will only be evaluated when executed" >> { implicit sc: SC =>
    val out = new StringBuffer
    val o = lazyObject({out.append("evaluated"); 1})
    "effect must not be evaluated" ==> { out.toString must be empty }
    o.run === 1
    "effect must be evaluated" ==> { out must not be empty }.unless(sc.isRemote)
  }

  tag("issue 266")
  "A DObject must not failed to be run when containing a large sequence of elements" >> { implicit sc: ScoobiConfiguration =>
    DObject((1 to 100000000).toStream.toSeq).run must terminate(sleep = 5.seconds)
  }


}

/**
  * Copyright 2011 National ICT Australia Limited
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

import scala.collection.mutable.{MutableList => MList}

import com.nicta.scoobi.io.Persister
import com.nicta.scoobi.impl.plan.Smart
import com.nicta.scoobi.impl.plan.Smart.ConvertInfo
import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.plan.MSCRGraph
import com.nicta.scoobi.impl.exec.Executor


/** A job that persists distrubted lists and computes distributed objects. */
final case class Job() {

  private val persisters: MList[DListPersister[_]] = MList.empty

  /** Add a distributed list to be persisted to this job. */
  def <<(output: DListPersister[_]): Job = { persisters += output; this }


  /** Run the job. */
  def run() = {
    DList.persist(persisters.toArray: _*)
    persisters.clear
  }
}


/** A class that specifies how to make a distributed list persistent. */
class DListPersister[A](val dl: DList[A], val persister: Persister[A])

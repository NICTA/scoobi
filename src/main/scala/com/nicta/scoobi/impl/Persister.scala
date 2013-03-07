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
package impl

import org.apache.commons.logging.LogFactory
import exec.{HadoopMode, InMemoryMode}
import core.{DObject, DList, Persistent}
import plan.comp._
import core.Mode._

class Persister(sc: core.ScoobiConfiguration) {
  private implicit val configuration = sc
  private implicit lazy val logger = LogFactory.getLog("scoobi.Persister")

  private val inMemoryMode = InMemoryMode()
  private val hadoopMode   = HadoopMode(sc)

  def persist[A](ps: Seq[Persistent[_]]) = {
    val asOne = Root(ps.map(_.getComp))
    sc.mode match {
      case InMemory        => inMemoryMode.execute(asOne)
      case Local | Cluster => hadoopMode  .execute(asOne)
    }
    ps
  }

  def persist[A](list: DList[A]) = {
    sc.mode match {
      case InMemory        => inMemoryMode.execute(list)
      case Local | Cluster => hadoopMode  .execute(list)
    }
    list
  }

  def persist[A](o: DObject[A]): A = {
    sc.mode match {
      case InMemory        => inMemoryMode.execute(o).asInstanceOf[A]
      case Local | Cluster => hadoopMode  .execute(o).asInstanceOf[A]
    }
  }

  // reset all previous memoisation
  def reset = {
    inMemoryMode.reset
    hadoopMode.reset
  }
}

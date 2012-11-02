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
package application

import org.apache.commons.logging.LogFactory

import core._
import Mode._
import impl.exec._

object Persister {
  lazy val logger = LogFactory.getLog("scoobi.Persister")

  def persist[A](list: DList[A])(implicit sc: ScoobiConfiguration) {
    sc.mode match {
      case InMemory        => InMemoryMode.execute(list)
      case Local | Cluster => HadoopMode().execute(list)
    }
  }

  def persist[A](o: DObject[A])(implicit sc: ScoobiConfiguration): A = {
    sc.mode match {
      case InMemory        => InMemoryMode.execute(o).asInstanceOf[A]
      case Local | Cluster => HadoopMode().execute(o).asInstanceOf[A]
    }
  }
}



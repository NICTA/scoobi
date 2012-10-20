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
package exec

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache

import application.ScoobiConfiguration
import core._

/** A reference to the storage location of a value that represents the "environment"
  * in which a computation is performed within. "Environment" here refers to the
  * the "side-input" of a "ParallelDo". Computed environments are pushed/pulled to/from
  * the distributed cache. */
class Env[E : WireFormat] private (path: Path) {

  /** Store the environment value in the distributed cache. */
  def push(conf: Configuration, env: E) {
    val dos = path.getFileSystem(conf).create(path)
    implicitly[WireFormat[E]].toWire(env, dos)
    dos.close()
    DistributedCache.addCacheFile(path.toUri, conf)
  }

  /** Get an environment value from the distributed cache. */
  def pull(conf: Configuration): E = {
    val cacheFiles = DistributedCache.getCacheFiles(conf)
    val cacheFile = new Path(cacheFiles.filter(_.toString.compareTo(path.toString) == 0)(0).toString)
    val dis = cacheFile.getFileSystem(conf).open(cacheFile)
    val obj: E = implicitly[WireFormat[E]].fromWire(dis)
    dis.close()
    obj
  }

  override def toString = "Env( " + path.toUri + ")"
}


object Env {

  /** Create a new "environment" container. */
  def apply[E](wf: WireFormat[E], conf: ScoobiConfiguration): Env[E] = {
    val id = java.util.UUID.randomUUID.toString
    val path = new Path(conf.workingDirectory, "env/" + id)
    new Env(path)(wf)
  }

  /** Create an "environment" container for the "Unit" value. */
  val empty: Env[Unit] = new Env[Unit](new Path("empty")) {
    override def push(conf: Configuration, env: Unit) {}
    override def pull(conf: Configuration) = ()
  }
}

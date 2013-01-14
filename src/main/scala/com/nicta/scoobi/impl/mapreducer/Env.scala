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
package mapreducer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache

import core._

/** A reference to the storage location of a value that represents the "environment"
  * in which a computation is performed within. "Environment" here refers to the
  * the "side-input" of a "ParallelDo". Computed environments are pushed/pulled to/from
  * the distributed cache. */
class Env(path: Path)(wf: WireReaderWriter) extends Environment {

  /** Store the environment value in the distributed cache. */
  def push(env: Any)(implicit configuration: Configuration) {
    val dos = path.getFileSystem(configuration).create(path)
    wf.write(env, dos)
    dos.close()
    DistributedCache.addCacheFile(path.toUri, configuration)
  }

  /** Get an environment value from the distributed cache. */
  def pull(implicit configuration: Configuration): Any = {
    val cacheFiles = DistributedCache.getCacheFiles(configuration)
    val cacheFilePaths = cacheFiles.filter(_.toString == path.toString)
    val cacheFilePath  = cacheFilePaths.headOption.
                         getOrElse(throw new Exception("\nno cache files contain the path: "+path+cacheFiles.mkString(" (\n  ", ",\n  ", ")")))

    val cacheFile = new Path(cacheFilePath.toString)
    val dis = cacheFile.getFileSystem(configuration).open(cacheFile)
    val obj = wf.read(dis)
    dis.close()
    obj
  }

  override def toString = "Env(" + path.toUri + ")"
}


object Env {

  /** Create a new "environment" container. */
  def apply(wf: WireReaderWriter)(implicit sc: ScoobiConfiguration): Env = {
    val id = java.util.UUID.randomUUID.toString
    val path = new Path(sc.workingDirectory, "env/" + id)
    new Env(path)(wf)
  }

  /** Create an "environment" container for the "Unit" value. */
  val empty: Env = new Env(new Path("empty"))(WireFormat.wireFormat[Unit]) {
    override def push(env: Any)(implicit c: Configuration) {}
    override def pull(implicit c: Configuration) {}
  }
}

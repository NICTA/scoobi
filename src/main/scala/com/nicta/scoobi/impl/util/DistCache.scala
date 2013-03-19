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
package util

import org.apache.hadoop.fs._
import org.apache.hadoop.filecache._
import org.apache.hadoop.conf.Configuration
import Configurations._

/** Faciliate making an object available to all tasks (mappers, reducers, etc). Use
  * XStream to serialise objects to XML strings and then send out via Hadoop's
  * distributed cache. Two APIs are provided for pushing and pulling objects. */
object DistCache {

  /** Make a local filesystem path based on a 'tag' to temporarily store the
    * serialised object. */
  def mkPath(configuration: Configuration, tag: String): Path = {
    val scratchDir = new Path(ScoobiConfigurationImpl(configuration).workingDirectory, "dist-objs")
    new Path(scratchDir, tag+ Option(configuration.get(JOB_STEP)).map("-"+_).getOrElse(""))
  }

  /** Distribute an object to be available for tasks in the current job. */
  def pushObject[T](configuration: Configuration, obj: T, tag: String): Path = {
    serialise[T](configuration, obj, tag) { path =>
      DistributedCache.addCacheFile(path.toUri, configuration)
    }
  }

  /**
   * serialise an object to a path
   */
  def serialise[T](configuration: Configuration, obj: T, tag: String)(action: Path => Unit): Path = {
    /* Serialise */
    val path = mkPath(configuration, tag)
    val dos = path.getFileSystem(configuration).create(path)
    Serialiser.serialise(obj, dos)
    action(path)
    new Path(new Path(path.getFileSystem(configuration).getUri), path)
  }

  /** Get an object that has been distributed so as to be available for tasks in
    * the current job. */
  def pullObject[T](configuration: Configuration, tag: String): Option[T] = {
    /* Get distributed cache file. */
    val path = mkPath(configuration, tag)
    val cacheFiles = DistributedCache.getCacheFiles(configuration)
    cacheFiles.find(_.toString == path.toString).flatMap { uri =>
      deserialise(configuration)(new Path(uri.toString))
    }
  }

  /**
   * deserialise an object from a path file
   */
  def deserialise[T](configuration: Configuration) = (path: Path) => {
    val dis = path.getFileSystem(configuration).open(path)
    try {
      Some(Serialiser.deserialise(dis).asInstanceOf[T])
    } catch { case e => { e.printStackTrace(); None } }
    finally { dis.close() }
  }
}


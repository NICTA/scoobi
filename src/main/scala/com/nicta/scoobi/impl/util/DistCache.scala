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
import ScoobiConfiguration._
import java.net.URI
import DistributedCache._
import java.io.IOException
import org.apache.commons.logging.LogFactory

/** Faciliate making an object available to all tasks (mappers, reducers, etc). Use
  * XStream to serialise objects to XML strings and then send out via Hadoop's
  * distributed cache. Two APIs are provided for pushing and pulling objects. */
object DistCache {
  private lazy val logger = LogFactory.getLog("scoobi.DistCache")

  /** Make a local filesystem path based on a 'tag' to temporarily store the
    * serialised object. */
  def mkPath(configuration: Configuration, tag: String): Path = {
    val scratchDir = new Path(ScoobiConfigurationImpl(configuration).workingDirectory, "dist-objs")
    new Path(scratchDir, tag+ Option(configuration.get(JOB_STEP)).map("-"+_).getOrElse(""))
  }

  /**
   * Distribute an object to be available for tasks in the current job
   *
   * By default check right away if the object can be deserialised
   */
  def pushObject[T](configuration: Configuration, obj: T, tag: String, check: Boolean = true): Path = {
    val path = serialise[T](configuration, obj, tag) { path =>
      DistributedCache.addCacheFile(path.toUri, configuration)
    }

    if (check)
      try Serialiser.deserialise(path.getFileSystem(configuration).open(path))
      catch { case e: Throwable => throw new IOException(s"The object $obj can not be serialised/deserialised: ${e.getMessage}", e) }

    path
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
    val path = mkPath(configuration, tag)

    lazy val remoteCacheFiles = Option(DistributedCache.getCacheFiles(configuration))getOrElse(Array[URI]())
    lazy val localCacheFiles = Option(DistributedCache.getLocalCacheFiles(configuration)).getOrElse(Array[Path]()).map(_.toUri)

    // use the local cached files on the cluster when the local files can be found
    val cacheFiles =
      if (localCacheFiles.nonEmpty) localCacheFiles
      else                          { logger.warn(path+" was not found in the local cache files"+localCacheFiles.mkString("\n", "\n", "\n"))
                                      remoteCacheFiles }

    cacheFiles.find(uri => uri.toString.endsWith(path.toString)).flatMap { case uri =>
      deserialise(configuration)(path)
    }
  }

  /**
   * deserialise an object from a path file
   */
  def deserialise[T](configuration: Configuration) = (path: Path) => {
    val dis = path.getFileSystem(configuration).open(path)
    try {
      Some(Serialiser.deserialise(dis).asInstanceOf[T])
    } catch { case e: Throwable => { e.printStackTrace(); None } }
    finally { dis.close() }
  }
}


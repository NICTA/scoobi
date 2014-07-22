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
import Compatibility._
import org.apache.hadoop.conf.Configuration
import Configurations._
import ScoobiConfiguration._
import java.net.URI
import java.io._
import org.apache.commons.logging.LogFactory
import control.Exceptions._

/**
 * Facilitate making an object available to all tasks (mappers, reducers, etc). Use
 * XStream to serialise objects to XML strings and then send out via Hadoop's
 * distributed cache. Two APIs are provided for pushing and pulling objects.
 */
object DistCache {
  private lazy val logger = LogFactory.getLog("scoobi.DistCache")

  /**
   * Make a local filesystem path based on a 'tag' to temporarily store the
   * serialised object.
   */
  def tagToPath(configuration: Configuration, tag: String): Path = {
    val sc = ScoobiConfigurationImpl(configuration)
    new Path(s"${sc.workingDirectory}/dist-objs/$tag")
  }

  /**
   * Distribute an object to be available for tasks in the current job
   */
  def pushObject[T](configuration: Configuration, obj: T, serialiser: (T, DataOutputStream) => Unit, tag: String): Path =
    serialise[T](configuration, obj, tag)(serialiser)

  /**
   * Distribute an object to be available for tasks in the current job using xstream
   *
   * By default check right away if the object can be deserialised
   */
  def pushObject[T](configuration: Configuration, obj: T, tag: String, check: Boolean = true): Path = {
    val path = pushObject[T](configuration, obj, (t: T, out: DataOutputStream) => Serialiser.serialise(t, out), tag)

    if (check)
      try Serialiser.deserialise(path.getFileSystem(configuration).open(path))
      catch { case e: Throwable => throw new IOException(s"The object $obj can not be serialised/deserialised: ${e.getMessage}", e) }

    path
  }

  /**
   * serialise an object to a path
   */
  private def serialise[T](configuration: Configuration, obj: T, tag: String)(serialiser: (T, DataOutputStream) => Unit): Path = {
    /* Serialise */
    val path = tagToPath(configuration, tag)
    val dos = path.getFileSystem(configuration).create(path)
    try serialiser(obj, dos)
    finally dos.close
    cache.addCacheFile(path.toUri, configuration)
    path
  }

  /** Get an object that has been distributed so as to be available for tasks in
    * the current job. */
  def pullObject[T](configuration: Configuration, tag: String, memoise: Boolean = false): Option[T] =
    pullPath(configuration, tagToPath(configuration, tag), memoise)(in => Serialiser.deserialise(in).asInstanceOf[T])

  /** Get an object that has been distributed so as to be available for tasks in
    * the current job. */
  def pullObjectDeserialise[T](configuration: Configuration, tag: String, deserialiser: DataInputStream => T, memoise: Boolean = false): Option[T] =
    pullPath(configuration, tagToPath(configuration, tag), memoise)(deserialiser)

  /** pull an object from the cache by passing the cache paths directly */
  def pullObject[T](cacheFiles: Array[Path], path: Path): Option[T] =
    pullFromPath(cacheFiles.toSeq, path, new Configuration) { dis =>
      Serialiser.deserialise(dis).asInstanceOf[T]
    }

  /**
   * Pulling an object from a given path.
   *
   * We first try paths from:
   *
   *  - the local cache then
   *  - the distributed cache then
   *  - the passed path itself (this is useful if we are on the client)
   *
   * Once a FSDataInputStream is successfully opened, the function f can do its job to recreate the object:
   *
   *  - use the Serialiser to deserialise the object
   *  - use a WireFormat to deserialise the object
   */
  def pullPath[T](configuration: Configuration, path: Path, memoise: Boolean = false)(f: FSDataInputStream => T): Option[T] =
    pullFromPath(localCacheFiles(configuration) ++ cacheFiles(configuration), path, configuration, memoise)(f)

  /** @return the list of local cache files */
  def localCacheFiles(configuration: Configuration) =
    Option(cache.getLocalCacheFiles(configuration)).getOrElse(Array[Path]()).map(p => new Path("file://"+p.toString))

  /** @return the list of cache files */
  def cacheFiles(configuration: Configuration) =
    Option(cache.getCacheFiles(configuration)).getOrElse(Array[URI]()).map(new Path(_))

  def pullFromPath[T](cacheFiles: Seq[Path], path: Path, configuration: Configuration = new Configuration, memoise: Boolean = false)(f: FSDataInputStream => T): Option[T] = {

    lazy val deserialiseObject: Option[T] = {
      val allFiles = (cacheFiles :+ path).distinct.toStream
      logger.info("trying to pull an object from the cache at path: "+path+s" (memoise=$memoise)")
      (allFiles :+ path).filter(p => p.toString.endsWith(path.getName)).map { case p =>
        logger.info("trying to open: "+p)
        tryo(p.getFileSystem(configuration).open(p)).map { dis =>
          logger.info("successfully opened: "+p)
          try f(dis)
          finally dis.close
        }
      }.dropWhile(!_.isDefined).flatten.headOption match {
        case Some(o) => Some(o)
        case None    => logger.error(allFiles.mkString("No successfully opened path. The cache files which were used are\n", "\n", "\n")); None
      }
    }

    if (memoise) deserialisedObjects.getOrElseUpdate(path.getName, deserialiseObject).asInstanceOf[Option[T]]
    else         deserialiseObject
  }

  private val deserialisedObjects = new scala.collection.mutable.WeakHashMap[String, Option[Any]]

}


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
package com.nicta.scoobi.impl.exec

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.filecache._
import com.thoughtworks.xstream.XStream

import com.nicta.scoobi.Scoobi


/** Faciliate making an object available to all tasks (mappers, reducers, etc). Use
  * XStream to serialize objects to XML strings and then send out via Hadoop's
  * distributed cache. Two APIs are provided for pushing and pulling objects. */
object DistCache {

  private val xstream = new XStream()

  /** Make a local filesystem path based on a 'tag' to temporarily store the
    * serialized object. */
  private def mkPath(conf: Configuration, tag: String): Path = {
    val scratchDir = new Path(Scoobi.getWorkingDirectory(conf), "dist-objs")
    new Path(scratchDir, tag)
  }

  /** Distribute an object to be available for tasks in the current job. */
  def pushObject[T](conf: Configuration, obj: T, tag: String): Unit = {
    /* Serialize */
    val path = mkPath(conf, tag)
    val dos = path.getFileSystem(Scoobi.conf).create(path)
    xstream.toXML(obj, dos)
    dos.close()

    /* Add as distributed cache file. */
    DistributedCache.addCacheFile(path.toUri, conf)
  }

  /** Get an object that has been distributed so as to be available for tasks in
    * the current job. */
  def pullObject[T : Manifest](conf: Configuration, tag: String): T = {
    /* Get distributed cache file. */
    val path = mkPath(conf, tag)
    val cacheFiles = DistributedCache.getCacheFiles(conf)
    val cacheFile = new Path(cacheFiles.filter(_.toString.compareTo(path.toString) == 0)(0).toString)

    /* Deserialize */
    val dis = cacheFile.getFileSystem(Scoobi.conf).open(cacheFile)
    val obj = xstream.fromXML(dis).asInstanceOf[T]
    dis.close()
    obj
  }
}


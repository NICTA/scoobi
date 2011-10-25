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

import java.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.fs._
import org.apache.hadoop.filecache._

import com.nicta.scoobi.Scoobi


/** Faciliate making an object available to all tasks (mappers, reducers, etc). The
  * basic idea is to use standard Java serialization and Hadoop's distributed cache.
  * Two APIs are provided for pushing and pulling objects. */
object DistributedObject {

  /** Make a local filesystem path based on a 'tag' to temporarily store the
    * serialized object. */
  private def mkPath(jobConf: JobConf, tag: String): Path = {
    val scratchDir = new Path(Scoobi.getWorkingDirectory(jobConf), "dist-objs")
    new Path(scratchDir, tag)
  }

  /** Distribute an object to be available for tasks in the current job. */
  def pushObject(jobConf: JobConf, obj: AnyRef, tag: String): Unit = {
    /* Serialize */
    val path = mkPath(jobConf, tag)
    val oos = new ObjectOutputStream(path.getFileSystem(Scoobi.conf).create(path))
    oos.writeObject(obj)
    oos.close()

    /* Add as distributed cache file. */
    DistributedCache.addCacheFile(path.toUri, jobConf)
  }

  /** Get an object that has been distributed so as to be available for tasks in
    * the current job. */
  def pullObject(jobConf: JobConf, tag: String): AnyRef = {
    /* Get distributed cache file. */
    val path = mkPath(jobConf, tag)
    val cacheFiles = DistributedCache.getCacheFiles(jobConf)
    val cacheFile = new Path(cacheFiles.filter(_.toString.compareTo(path.toString) == 0)(0).toString)

    /* Deserialize */
    val ois = new ObjectInputStream(cacheFile.getFileSystem(Scoobi.conf).open(cacheFile))
    val obj = ois.readObject().asInstanceOf[AnyRef]
    ois.close()
    obj
  }
}


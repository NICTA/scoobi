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

import java.net.{URLClassLoader, URL}
import com.nicta.scoobi.io.FileSystems
import java.io.File
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.Path

/**
 * This trait defines:
 *
 * - the library jars which can be uploaded to the cluster
 * - a method to upload and reference them on the classpath for cluster jobs
 */
trait LibJars {

  /**
   * @return the name of the directory to use when loading jars to the filesystem.
   *         the path which will be used will be relative to the user home on the cluster
   */
  def libjarsDirectory = "libjars/"

  /** this variable controls if the upload must be done at all */
  def upload = true

  /**
   * @return the list of library jars to upload, provided by the jars loaded by the current classloader
   */
  def jars: Seq[URL] = Thread.currentThread.getContextClassLoader.asInstanceOf[URLClassLoader].getURLs.filter { url =>
    !url.getFile.contains("hadoop-core") &&
    (Seq(".ivy2", ".m2").exists(url.getFile.contains) || url.getFile.contains("scala-library"))
  }

  /**
   * @return the remote jars currently on the cluster
   */
  def uploadedJars(implicit configuration: ScoobiConfiguration): Seq[Path] = FileSystems.listFiles(libjarsDirectory)

  /**
   * @return delete the remote jars currently on the cluster
   */
  def deleteJars(implicit configuration: ScoobiConfiguration) { FileSystems.deleteFiles(libjarsDirectory) }

  /**
   * upload the jars which don't exist yet in the library directory on the cluster
   */
  def uploadLibJarsFiles(implicit configuration: ScoobiConfiguration) = if (upload) {
    FileSystems.mkdir(libjarsDirectory)
    FileSystems.uploadNewJars(jars.map(url => new File(url.getFile)), libjarsDirectory)
    configureJars
  }

  /**
   * @return a configuration where the appropriate properties are set-up for uploaded jars: distributed files + classpath
   */
  def configureJars(implicit configuration: ScoobiConfiguration) = if (upload) {
    uploadedJars.foreach(path => DistributedCache.addFileToClassPath(path, configuration))
    configuration.addValues("mapred.classpath", jars.map(j => libjarsDirectory + (new File(j.getFile).getName)), ":")
  }
}


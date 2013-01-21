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
import java.io.File
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.Path
import core._
import impl.io.FileSystems
import impl.ScoobiConfiguration._
import impl.ScoobiConfigurationImpl._
import org.apache.commons.logging.LogFactory
import impl.control.SystemProperties
import impl.monitor.Loggable._
/**
 * This trait defines:
 *
 * - the library jars which can be uploaded to the cluster
 * - a method to upload and reference them on the classpath for cluster jobs
 */
trait LibJars {
  private implicit lazy val logger = LogFactory.getLog("scoobi.LibJars")

  protected[scoobi] lazy val fss: FileSystems = FileSystems
  protected[scoobi] lazy val sysProps: SystemProperties = SystemProperties

  /**
   * @return the path of the directory to use when loading jars to the filesystem.
   */
  def libjarsDirectory = fss.dirPath(sysProps.get("scoobi.libjarsdir").getOrElse("libjars"))

  /** this variable controls if the upload must be done at all */
  def upload: Boolean = true

  /**
   * @return the list of library jars to upload
   */
  def jars: Seq[URL] = classLoaderJars ++ hadoopClasspathJars

  /**
   * @return the list of library jars to upload, provided by the jars loaded by the current classloader
   */
  private[scoobi]
  lazy val classLoaderJars: Seq[URL] =
    Thread.currentThread.getContextClassLoader.asInstanceOf[URLClassLoader].getURLs.filter(url => !url.getFile.contains("hadoop-core")).
      debugNot(_.isEmpty, jars => "jars found with the classloader\n"+jars.mkString("\n"))


  /**
   * @return the list of library jars to upload, provided by the jars found from the HADOOP_CLASSPATH variable
   */
  private[scoobi]
  def hadoopClasspathJars: Seq[URL] = hadoopClasspaths.flatMap(jarsOnPath).
    debugNot(_.isEmpty, jars => "the jars found with the $HADOOP_CLASSPATH variable are\n"+jars.mkString("\n"))

  /** @return the list of paths on the HADOOP_CLASSPATH variable */
  private[scoobi]
  def hadoopClasspaths = sysProps.getEnv("HADOOP_CLASSPATH").orElse(None.debug(_ => true, _ => "HADOOP_CLASSPATH variable is not set")).
                         map(_.split(File.pathSeparatorChar).toSeq).getOrElse(Seq())

  /** @return the list of jars for a given path, either a single jar or all the jars in a directory */
  private[scoobi]
  def jarsOnPath(path: String): Seq[URL] = {
    if (path.endsWith(".jar"))    Seq(new File(path).toURI.toURL)
    else if (path.endsWith("/*")) fss.listFilePaths(path.replace("/*", "")).flatMap(jarsOnPath)
    else                          Seq()
  }
  /**
   * @return the remote jars currently on the cluster
   */
  def uploadedJars(implicit configuration: ScoobiConfiguration): Seq[Path] = fss.listPaths(libjarsDirectory)

  /**
   * @return delete the remote jars currently on the cluster
   */
  def deleteJars(implicit configuration: ScoobiConfiguration) { fss.deleteFiles(libjarsDirectory) }

  /**
   * upload the jars which don't exist yet in the library directory on the cluster
   */
  def uploadLibJarsFiles(deleteLibJarsFirst: Boolean = false)(implicit configuration: ScoobiConfiguration) = {
    if (deleteLibJarsFirst) {
      logger.debug("delete existing lib jars on the cluster")
      deleteJars
    }

    if (upload) {
      logger.debugNot(fss.exists(libjarsDirectory), "creating a libjars directory at "+libjarsDirectory+" (file system is remote: "+(!fss.isLocal)+")")
      fss.mkdir(libjarsDirectory)

      val jarFiles = jars.map(url => new File(url.getFile)).filter(f => f.exists && !f.isDirectory)

      fss.uploadNewJars(jarFiles, libjarsDirectory)
      configureJars
    } else logger.debug("no jars are uploaded because upload=false")
  }


  /**
   * @return a configuration where the appropriate properties are set-up for uploaded jars: distributed files + classpath
   */
  def configureJars(implicit configuration: ScoobiConfiguration) = if (upload) {
    logger.debug("adding the jars paths to the distributed cache")
    uploadedJars.foreach(path => DistributedCache.addFileToClassPath(path, configuration))

    logger.debug("adding the jars classpaths to the mapred.classpath variable")
    configuration.addValues("mapred.classpath", jars.map(j => libjarsDirectory + (new File(j.getFile).getName)), ":")
  }
}
object LibJars extends LibJars

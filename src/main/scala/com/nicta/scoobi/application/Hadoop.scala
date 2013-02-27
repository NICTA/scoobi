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

import org.apache.hadoop.fs.FileSystem
import core._
import Mode._
import org.apache.commons.logging.LogFactory
import impl.ScoobiConfiguration._
/**
 * This trait provides methods to execute map-reduce code, either locally or on the cluster.
 *
 * @see LocalHadoop
 *
 * To use this trait, you need to provide:
 *
 *  - the file system address: def fs = "hdfs://svm-hadoop1.ssrg.nicta.com.au"
 *  - the job tracker address: def jobTracker = "svm-hadoop1.ssrg.nicta.com.au:8021"
 */
trait Hadoop extends LocalHadoop with Cluster with LibJars { outer =>
  private lazy val logger = LogFactory.getLog("scoobi.Hadoop")

  /** @return true if you want to include the library jars in the jar that is sent to the cluster for each job */
  def includeLibJars = false

  /** @return the classes directories to include on a job classpath */
  def classDirs: Seq[String] = Seq("classes", "test-classes").map("target/scala-"+util.Properties.releaseVersion.getOrElse("2.9.2")+"/"+_)

  /** execute some code on the cluster, possibly showing the execution time */
  def onCluster[T](t: =>T)(implicit configuration: ScoobiConfiguration) =
    showTime(executeOnCluster(t))(displayTime("Cluster execution time"))

  /** execute some code, either locally or on the cluster, depending on the local argument being passed on the commandline */
  def onHadoop[T](t: =>T)(implicit configuration: ScoobiConfiguration) =
    if (isInMemory)   inMemory(t)
    else if (isLocal) onLocal(t)
    else              onCluster(t)


  /**
   * execute some code on the cluster, setting the filesystem / jobtracker addresses and setting up the classpath
   */
  def executeOnCluster[T](t: =>T)(implicit configuration: ScoobiConfiguration) = {
    configureForCluster
    runOnCluster(t)
  }

  /**
   * @return a configuration with cluster setup
   */
  def configureForCluster(implicit configuration: ScoobiConfiguration): ScoobiConfiguration = {
    setLogFactory()

    logger.debug("setting the configuration for cluster execution")

    if (!configuration.jobName.isDefined) configuration.jobNameIs(getClass.getSimpleName)

    logger.debug("setting the execution mode to "+Cluster)
    configuration.modeIs(Cluster)

    logger.debug("the file system is "+fs)
    configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, fs)

    logger.debug("the job tracker is "+jobTracker)
    configuration.set("mapred.job.tracker", jobTracker)

    logger.debug("jars have been uploaded "+outer.upload)
    configuration.setUploadedLibJars(uploaded = outer.upload)

    logger.debug("setting the mapreduce classpath so that the user classpath takes precedence")
    configuration.setBoolean("mapreduce.task.classpath.user.precedence", true)

    // include libjars in the ScoobiJob jar
    if (includeLibJars) {
      logger.debug("include jars passed with the -libjars arguments")
      configuration.includeLibJars(jars)
    }

    configureJars
    configuration.addUserDirs(classDirs)
    configuration.setDirectories
  }

  /**
   * @return the cluster evaluation of t
   */
  def runOnCluster[T](t: =>T) = t

}

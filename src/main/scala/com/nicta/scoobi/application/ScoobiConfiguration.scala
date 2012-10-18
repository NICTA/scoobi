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

import impl.Configurations
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._
import Configurations._
import java.net.URL
import org.apache.hadoop.mapred.JobConf
import java.io.File
import ScoobiConfiguration._
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FileSystem._
import io.FileSystems
import impl.reflect.Classes

/**
 * This class wraps the Hadoop (mutable) configuration with additional configuration information such as the jars which should be
 * added to the classpath.
 */
case class ScoobiConfiguration(configuration: Configuration = new Configuration,
                               var userJars: Set[String] = Set(),
                               var userDirs: Set[String] = Set()) {


  /* By default use vector mode. */
  var mode: Boolean = true  // TODO - control this via the command line, etc

  /**Parse the generic Hadoop command line arguments, and call the user code with the remaining arguments */
  def withHadoopArgs(args: Array[String])(f: Array[String] => Unit): ScoobiConfiguration = callWithHadoopArgs(args, f)

  /** Helper method that parses the generic Hadoop command line arguments before
   * calling the user's code with the remaining arguments. */
  private def callWithHadoopArgs(args: Array[String], f: Array[String] => Unit): ScoobiConfiguration = {
    /* Parse options then update current configuration. Because the filesystem
     * property may have changed, also update working directory property. */
    val parser = new GenericOptionsParser(configuration, args)
    /* Run the user's code */
    f(parser.getRemainingArgs)
    this
  }

  /** get the default values from the configuration files */
  def loadDefaults = {
    new GenericOptionsParser(configuration, Array[String]())
    this
  }

  /** add a list of jars to include as -libjars in this configuration */
  def includeLibJars(jars: Seq[URL]) = parse("libjars", jars.map(_.getFile).mkString(","))

  /**
   * use the GenericOptionsParser to parse the value of a command line argument and update the current configuration
   * The command line argument doesn't have to start with a dash.
   */
  def parse(commandLineArg: String, value: String) = {
    new GenericOptionsParser(configuration, Array((if (!commandLineArg.startsWith("-")) "-" else "") + commandLineArg, value))
    this
  }

  /**
   * add a new jar url (as a String) to the current configuration
   */
  def addJar(jar: String) = { userJars = userJars + jar; this }

  /**
   * add several user jars to the classpath of this configuration
   */
  def addJars(jars: Seq[String]) = jars.foldLeft(this) {
    (result, jar) => result.addJar(jar)
  }

  /**
   * add a new jar of a given class, by finding the url in the current classloader, to the current configuration
   */
  def addJarByClass(clazz: Class[_]) = Classes.findContainingJar(clazz).map(addJar).getOrElse(this)

  /**
   * add a user directory to the classpath of this configuration
   */
  def addUserDir(dir: String) = { userDirs = userDirs + withTrailingSlash(dir); this }

  /**
   * add several user directories to the classpath of this configuration
   */
  def addUserDirs(dirs: Seq[String]) = dirs.foldLeft(this) {
    (result, dir) => result.addUserDir(dir)
  }

  /**
   * @return true if this configuration is used for a remote job execution
   */
  def isRemote = configuration.getBoolean("scoobi.remote", false)

  /**
   * set a flag in order to know that this configuration is for a remote execution
   */
  def setRemote(remote: Boolean) {
    set("scoobi.remote", remote.toString)
  }
  /**
   * @return true if the dependent jars have been uploaded
   */
  def uploadedLibJars = configuration.getBoolean("scoobi.uploadedlibjars", false)

  /**
   * set a flag in order to know if jars have been uploaded before jobs are defined
   */
  def setUploadedLibJars(uploaded: Boolean) {
    set("scoobi.uploadedlibjars", uploaded.toString)
  }

  /** Set an upper bound for the number of reducers to be used in M/R jobs */
  def setMaxReducers(maxReducers: Int) {
    configuration.setInt("scoobi.mapreduce.reducers.max", maxReducers)
  }

  /** Get the max number of reducers to use in M/R jobs */
  def getMaxReducers = configuration.getInt("scoobi.mapreduce.reducers.max", Int.MaxValue)

  /** Set a lower bound for the number of reducers to be used in M/R jobs */
  def setMinReducers(minReducers: Int) {
    configuration.setInt("scoobi.mapreduce.reducers.min", minReducers)
  }

  /** Get the min number of reducers to use in M/R jobs */
  def getMinReducers = configuration.getInt("scoobi.mapreduce.reducers.min", 1)

  /**
   * Set the number of input bytes per reducer. This is used to control the number of
   * reducers based off the size of the input data to the M/R job.
   */
  def setBytesPerReducer(sizeInBytes: Long) {
    configuration.setLong("scoobi.mapreduce.reducers.bytesperreducer", sizeInBytes)
  }

  /**
   * Get the number of input bytes per reducer. Default is 1GiB.
   */
  def getBytesPerReducer = configuration.getLong("scoobi.mapreduce.reducers.bytesperreducer", 1024 * 1024 * 1024)

  /**
   * set a new job name to help recognize the job better
   */
  def jobNameIs(name: String) {
    set("scoobi.jobname", name)
  }

  /**
   * the file system for this configuration
   */
  lazy val fs = FileSystem.get(configuration)

  /**
   * @return the job name if one is defined
   */
  def jobName: Option[String] = Option(configuration.get("scoobi.jobname"))

  /* Timestamp used to mark each Scoobi working directory. */
  private def timestamp = {
    val now = new Date
    val sdf = new SimpleDateFormat("yyyyMMdd-HHmmss")
    sdf.format(now)
  }

  /** The id for the current Scoobi job being (or about to be) executed. */
  lazy val jobId: String = (Seq("scoobi", timestamp) ++ jobName :+ uniqueId).mkString("-")
  /** The job name for a step in the current Scoobi, i.e. a single MapReduce job */
  def jobStep(stepId: Int) = jobId + "(Step-" + stepId + ")"

  /**Scoobi's configuration. */
  lazy val conf = {
    configuration.set("scoobi.jobid", jobId)
    configuration.setInt("scoobi.progress.time", 500)
    // this setting avoids unnecessary warnings
    configuration.set("mapred.used.genericoptionsparser", "true")
    configuration
  }

  /**
   * force a configuration to be a local one
   */
  def setAsLocal: ScoobiConfiguration = {
    jobNameIs(getClass.getSimpleName)
    set(FS_DEFAULT_NAME_KEY, DEFAULT_FS)
    set("mapred.job.tracker", "local")
    setDirectories
  }

  /**
   * this setup needs to be done only after the internal conf object has been set to a local configuration or a cluster one *
   * because all the paths will depend on that
   */
  def setDirectories = {
    configuration.set("mapreduce.jobtracker.staging.root.dir", defaultWorkDir + "staging/")
    // before the creation of the input we set the mapred local dir.
    // this setting is necessary to avoid xml parsing when several scoobi jobs are executing concurrently and
    // trying to access the job.xml file
    configuration.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY, defaultWorkDir + "localRunner/")
    configuration.update("scoobi.workdir", defaultWorkDir)
    this
  }

  /**@return a pseudo-random unique id */
  private def uniqueId = java.util.UUID.randomUUID

  /** set a value on the configuration */
  def set(key: String, value: Any) {
    configuration.set(key, if (value == null) "null" else value.toString)
  }

  private lazy val scoobiTmpDir = FileSystem.get(configuration).getHomeDirectory.toUri.getPath + "/.scoobi-tmp/"
  private lazy val defaultWorkDir = withTrailingSlash(scoobiTmpDir + jobId)

  private def withTrailingSlash(s: String) = if (s endsWith "/") s else s + '/'

  lazy val workingDirectory: Path         = new Path(defaultWorkDir)
  lazy val temporaryOutputDirectory: Path = new Path(workingDirectory, "tmp-out")
  lazy val temporaryJarFile: File         = File.createTempFile("scoobi-job-"+jobId, ".jar")

  def deleteScoobiTmpDirectory = fs.delete(new Path(scoobiTmpDir), true)
  def deleteWorkingDirectory = fs.delete(new Path(defaultWorkDir), true)

  /** @return the file system for this configuration, either a local or a remote one */
  def fileSystem = FileSystems.fileSystem(this)
}

object ScoobiConfiguration {
  implicit def toExtendedConfiguration(sc: ScoobiConfiguration): ExtendedConfiguration = extendConfiguration(sc)

  implicit def toConfiguration(sc: ScoobiConfiguration): Configuration = sc.conf
  implicit def fromConfiguration(c: Configuration): ScoobiConfiguration = ScoobiConfiguration(c)

  def apply(args: Array[String]): ScoobiConfiguration = ScoobiConfiguration().callWithHadoopArgs(args, (a: Array[String]) => ())
}

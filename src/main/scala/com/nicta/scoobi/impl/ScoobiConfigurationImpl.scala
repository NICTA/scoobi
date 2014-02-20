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

import java.util.Date
import java.text.SimpleDateFormat
import java.net.URL
import java.io.File
import scala.collection.JavaConversions._
import mapreducer.Env
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.fs.FileSystem._
import org.apache.commons.logging.LogFactory._
import org.apache.hadoop.mapreduce.{Counters => HadoopCounters, Counter, Job}

import core._
import reflect.Classes
import io.FileSystems

import Configurations._
import FileSystems._
import monitor.Loggable._
import tools.nsc.interpreter.AbstractFileClassLoader
import com.nicta.scoobi.impl.util.Compatibility
import com.nicta.scoobi.core.Mode.{Local, InMemory}
import org.apache.commons.codec.binary.Base64

case class ScoobiConfigurationImpl(private val hadoopConfiguration: Configuration = new Configuration,
                                   var userJars: Set[String] = Set(),
                                   var userDirs: Set[String] = Set(),
                                   var classLoader: Option[AbstractFileClassLoader] = None,
                                   counters: HadoopCounters = new HadoopCounters) extends ScoobiConfiguration {

  /**
   * This call is necessary to load the mapred-site.xml properties file containing the address of the default job tracker
   * When creating a new JobConf object the mapred-site.xml file is going to be added as a new default resource and all
   * existing configuration objects are going to be reloaded with new properties
   */
  loadMapredSiteProperties
  def loadMapredSiteProperties = new JobConf

  private implicit lazy val logger = getLog("scoobi.ScoobiConfiguration")


  /** Scoobi's configuration, initialised with a job id */
  lazy val configuration = {
    hadoopConfiguration.set(JOB_ID, jobId)
    hadoopConfiguration.setInt(PROGRESS_TIME, 500)
    // this setting avoids unnecessary warnings
    hadoopConfiguration.set("mapred.used.genericoptionsparser", "true")
    hadoopConfiguration
  }

  /**
   * @return the job name if one is defined
   */
  def jobName: Option[String] = Option(hadoopConfiguration.get(JOB_NAME))

  /* Timestamp used to mark each Scoobi working directory. */
  private lazy val timestamp = {
    val now = new Date
    val sdf = new SimpleDateFormat("MMdd-HHmmss")
    hadoopConfiguration.getOrSet(JOB_TIMESTAMP, sdf.format(now))
  }

  /** The id for the current Scoobi job being (or about to be) executed. */
  def jobId: String = (jobName.toSeq ++ Seq(timestamp, uniqueId)).mkString("-")

  /** The job name for a step in the current Scoobi, i.e. a single MapReduce job */
  def jobStepIs(stepId: Int, stepsNumber: Int) = {
    // no space in the step name because it will be used in paths and spaces could cause problems
    configuration.set(JOB_STEP, s"step_${stepId}_of_$stepsNumber")
    configuration.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY, workingDir+configuration.get(JOB_STEP))
    jobStep
  }
  /** @return the job step */
  def jobStep =
    Option(configuration.get(JOB_STEP)).getOrElse("-").debug("the job step is")

  /** update the current counters from the result of a job that has just been run */
  def updateCounters(hadoopCounters: HadoopCounters) = {
    hadoopCounters.getGroupNames.map { groupName: String =>
      val group = hadoopCounters.getGroup(groupName)
      group.iterator.foreach((c: Counter) => counters.findCounter(groupName, c.getName).increment(c.getValue))
    }
    this
  }

  /**Parse the generic Hadoop command line arguments, and call the user code with the remaining arguments */
  def withHadoopArgs(args: Array[String])(f: Array[String] => Unit): ScoobiConfiguration = callWithHadoopArgs(args, f)

  /** Helper method that parses the generic Hadoop command line arguments before
    * calling the user's code with the remaining arguments. */
  private[scoobi] def callWithHadoopArgs(args: Array[String], f: Array[String] => Unit): ScoobiConfiguration = {
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

  /** @return user classes to add in the job jar, by class name (and corresponding bytecode) */
  def userClasses: Map[String, Array[Byte]] = classLoader.map(Classes.loadedClasses).getOrElse(Map())

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
  def addUserDir(dir: String) = { userDirs = userDirs + dirPath(dir); this }

  /**
   * add several user directories to the classpath of this configuration
   */
  def addUserDirs(dirs: Seq[String]) = dirs.foldLeft(this) {
    (result, dir) => result.addUserDir(dir)
  }

  /**
   * attach a classloader which classes must be put on the job classpath
   */
  def addClassLoader(cl: AbstractFileClassLoader) = { classLoader = Some(cl); configuration.setClassLoader(cl); this }

  /** @return the class loader to use with Scoobi classes, including generated ones */
  def scoobiClassLoader = classLoader.getOrElse(Thread.currentThread.getContextClassLoader)
  /**
   * @return true if this configuration is used for a remote job execution
   */
  def isRemote = mode == Mode.Cluster

  /**
   * @return true if this configuration is used for a local memory execution
   */
  def isLocal = mode == Mode.Local

  /**
   * @return true if this configuration is used for an in memory execution with a collection backend
   */
  def isInMemory = mode == Mode.InMemory

  /**
   * set a flag in order to know that this configuration is for a in-memory, local or remote execution,
   */
  def modeIs(mode: Mode) = {
    logger.debug("setting the scoobi execution mode: "+mode)

    set(SCOOBI_MODE, mode.toString)
    this
  }
  /** @return the current mode */
  def mode = Mode.unsafeWithName(configuration.get(SCOOBI_MODE, Mode.Local.toString))

  /** @return true if the mscr jobs can be executed concurrently */
  def concurrentJobs = hadoopConfiguration.getOrSetBoolean(CONCURRENT_JOBS, false)

  /** set to true if the mscr jobs must be executed concurrently */
  def setConcurrentJobs(concurrent: Boolean) = { hadoopConfiguration.setBoolean(CONCURRENT_JOBS, concurrent); this }

  /**
   * @return true if the dependent jars have been uploaded
   */
  def uploadedLibJars = configuration.getBoolean(UPLOADED_LIBJARS, false)

  /**
   * set a flag in order to know if jars have been uploaded before jobs are defined
   */
  def setUploadedLibJars(uploaded: Boolean) {
    set(UPLOADED_LIBJARS, uploaded.toString)
  }

  /** do not use combiners */
  def disableCombiners: ScoobiConfiguration = {
    configuration.setBoolean(DISABLE_COMBINERS, true)
    this
  }

  /** use combiners */
  def enableCombiners: ScoobiConfiguration = {
    configuration.setBoolean(DISABLE_COMBINERS, false)
    this
  }

  /** @return true if combiners are disabled */
  def disabledCombiners = configuration.getBoolean(DISABLE_COMBINERS, false)

  /** Set an upper bound for the number of reducers to be used in M/R jobs */
  def setMaxReducers(maxReducers: Int) {
    configuration.setInt(MAPREDUCE_REDUCERS_MAX, maxReducers)
  }

  /** Get the max number of reducers to use in M/R jobs */
  def getMaxReducers = configuration.getInt(MAPREDUCE_REDUCERS_MAX, Int.MaxValue)

  /** Set a lower bound for the number of reducers to be used in M/R jobs */
  def setMinReducers(minReducers: Int) {
    configuration.setInt(MAPREDUCE_REDUCERS_MIN, minReducers)
  }

  /** Get the min number of reducers to use in M/R jobs */
  def getMinReducers = configuration.getInt(MAPREDUCE_REDUCERS_MIN, 1)

  /**
   * Set the number of input bytes per reducer. This is used to control the number of
   * reducers based off the size of the input data to the M/R job.
   */
  def setBytesPerReducer(sizeInBytes: Long) {
    configuration.setLong(MAPREDUCE_REDUCERS_BYTESPERREDUCER, sizeInBytes)
  }

  /**
   * Get the number of input bytes per reducer. Default is 1GiB.
   */
  def getBytesPerReducer = configuration.getLong(MAPREDUCE_REDUCERS_BYTESPERREDUCER, 1024 * 1024 * 1024)

  /**
   * if true count the number of values per mapper
   */
  def setCountValuesPerMapper(b: Boolean) { configuration.setBoolean(COUNT_MAPPER_VALUES, b) }

  /**
   * @return true if we must count the number of values per mapper
   */
  def countValuesPerMapper: Boolean = configuration.getBoolean(COUNT_MAPPER_VALUES, false)

  /**
   * if true count the number of values per reducer
   */
  def setCountValuesPerReducer(b: Boolean) { configuration.setBoolean(COUNT_REDUCER_VALUES, b) }

  /**
   * @return true if we must count the number of values per reducer
   */
  def countValuesPerReducer: Boolean = configuration.getBoolean(COUNT_REDUCER_VALUES, false)

  /**
   * set a new job name to help recognize the job better
   */
  def jobNameIs(name: String) {
    set(JOB_NAME, name)
  }

  /** @return the file system for this configuration, either a local or a remote one */
  def fileSystem = FileSystems.fileSystem(this)

  /**
   * force a configuration to be an in-memory one, currently doing everything as in the local mode
   */
  def setAsInMemory: ScoobiConfiguration = {
    logger.debug("setting the ScoobiConfiguration as InMemory")
    setDefaultForInMemoryAndLocal
    modeIs(InMemory)
  }

  /**
   * force a configuration to be a local one
   */
  def setAsLocal: ScoobiConfiguration = {
    logger.debug("setting the ScoobiConfiguration as Local")
    setDefaultForInMemoryAndLocal
    modeIs(Local)
  }

  private def setDefaultForInMemoryAndLocal = {
    set(Compatibility.defaultFSKeyName, "file:///")
    set("mapred.job.tracker", "local")
    setDirectories
  }

  /**
   * this setup needs to be done only after the internal conf object has been set to a local configuration or a cluster one
   * because all the paths will depend on that
   */
  def setDirectories = {

    logger.debug("the mapreduce.jobtracker.staging.root.dir is "+workingDir + "staging/")
    configuration.set("mapreduce.jobtracker.staging.root.dir", workingDir + "staging/")
    // before the creation of the input we set the mapred local dir.
    // this setting is necessary to avoid xml parsing when several scoobi jobs are executing concurrently and
    // trying to access the job.xml file
    logger.debug("the "+JobConf.MAPRED_LOCAL_DIR_PROPERTY+" is "+workingDir + "localRunner/")
    configuration.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY, workingDir + "localRunner/")
    this
  }

  /**
   * @return a pseudo-random unique id
   *         we just need a bit of entropy here since the full Scoobi job id will have:
   *
   *         - the job name
   *         - the date/time
   *         - this id
   *
   * (and each Hadoop job name will have the mscr id appended to the Scoobi job id)
   *
   * This additional entropy is particularly useful when running tests to ensure that each one has it own unique
   * working directory
   */
  private lazy val uniqueId = hadoopConfiguration.getOrSet(JOB_UNIQUEID, java.util.UUID.randomUUID.hashCode.toString)

  /** set a value on the configuration */
  def set(key: String, value: Any) {
    configuration.set(key, if (value == null) "null" else value.toString)
  }

  def setScoobiDir(dir: String)      = { set("scoobi.dir", dirPath(dir)); this }

  def defaultScoobiDir                    = dirPath("/tmp/scoobi-"+sys.props.get("user.name").getOrElse("user"))
  lazy val scoobiDir                      = configuration.getOrSet("scoobi.dir", defaultScoobiDir)
  lazy val workingDir                     = configuration.getOrSet("scoobi.workingdir", dirPath(scoobiDir + jobId))
  lazy val scoobiDirectory: Path          = new Path(scoobiDir)
  lazy val workingDirectory: Path         = new Path(workingDir)
  def temporaryOutputDirectory            = new Path(workingDirectory, "tmp-out-"+hadoopConfiguration.get(JOB_STEP)+"/")
  lazy val temporaryJarFile: File         = File.createTempFile("scoobi-job-"+jobId, ".jar")

  def deleteScoobiDirectory          = fileSystem.delete(scoobiDirectory, true)
  def deleteWorkingDirectory         = fileSystem.delete(workingDirectory, true)
  def deleteTemporaryOutputDirectory = fileSystem.delete(temporaryOutputDirectory, true)

  /** @return a new environment object */
  def newEnv(wf: WireReaderWriter): Environment = Env(wf)(this)

  private val persister = new Persister(this)

  def persist[A](ps: Seq[Persistent[_]]) = persister.persist(ps)
  def persist[A](list: DList[A])         = persister.persist(list)
  def persist[A](o: DObject[A]): A       = persister.persist(o)

  def duplicate = {
    val c = new Configuration(configuration)
    val duplicated = ScoobiConfigurationImpl(c).addUserDirs(userDirs.toSeq).addJars(userJars.toSeq)
    classLoader.map(duplicated.addClassLoader).getOrElse(duplicated)
  }
}

object ScoobiConfigurationImpl {
  implicit def toExtendedConfiguration(sc: ScoobiConfiguration): ExtendedConfiguration = extendConfiguration(sc.configuration)

  // only used in tests
  private[scoobi] def unitEnv(configuration: Configuration) =
    new ScoobiConfigurationImpl(configuration) {
      override def newEnv(wf: WireReaderWriter) = new Environment {
        def push(any: Any)(implicit conf: Configuration) {}
        def pull(implicit conf: Configuration) = ((),())
      }
    }
}

trait ScoobiConfigurations {
  implicit def toConfiguration(sc: ScoobiConfiguration): Configuration = sc.configuration
  implicit def fromConfiguration(c: Configuration): ScoobiConfiguration = ScoobiConfigurationImpl(c)
  def apply(configuration: Configuration) = new ScoobiConfigurationImpl(configuration)
  def apply() = new ScoobiConfigurationImpl(new Configuration)
  def apply(args: Array[String]): ScoobiConfiguration =
    ScoobiConfigurationImpl(new Configuration()).callWithHadoopArgs(args, (a: Array[String]) => ())
}
object ScoobiConfiguration extends ScoobiConfigurations


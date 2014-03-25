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

import java.lang.reflect.Constructor
import java.lang.reflect.Method

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.io.SequenceFile
import core.ScoobiConfiguration
import org.apache.commons.logging.LogFactory
import com.nicta.scoobi.impl.io.Files
import java.net.URI
import scala.reflect.ClassTag


/**
 * Provides a compatibility layer for handling API incompatibilities between
 * hadoop 1.x and 2.x via reflection.
 */
object Compatibility {
  private lazy val logger = LogFactory.getLog("scoobi.Compatibility")

  private case class CDH4(useCdh4: Boolean,
                        taskAttemptContextConstructor: Constructor[_],
                        mapContextConstructor: Constructor[_],
                        jobContextConstructor: Constructor[_],
                        jobConstructor1:       Constructor[_],
                        jobConstructor2:       Constructor[_],
                        sequenceFileReaderConstructor: Constructor[_],
                        isDirectory: Method,
                        getConfiguration: Method)

  private lazy val cdh4: CDH4 = {
    // use the presence of JobContextImpl as a test for cdh4 classes
    val useCdh4 = try { Class.forName("org.apache.hadoop.mapreduce.task.JobContextImpl"); logger.debug("Hadoop CDH4 compatibility class is being used"); true }
                  catch { case _: Throwable => logger.debug("Hadoop CDH3 compatibility class is being used"); false }

    try {
      val taskAttemptContextClass =
        Class.forName(if (useCdh4) "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl"
                      else       "org.apache.hadoop.mapreduce.TaskAttemptContext")
      val taskAttemptContextConstructor = taskAttemptContextClass.getConstructor(classOf[Configuration], classOf[TaskAttemptID])

      val mapContextClass =
        Class.forName(if (useCdh4) "org.apache.hadoop.mapreduce.task.MapContextImpl"
        else       "org.apache.hadoop.mapreduce.MapContext")
      val mapContextConstructor = mapContextClass.getConstructor(classOf[Configuration], classOf[TaskAttemptID],
        classOf[RecordReader[_,_]], classOf[RecordWriter[_,_]],
        classOf[OutputCommitter], classOf[StatusReporter], classOf[InputSplit])

      val jobContextClass =
        Class.forName(if (useCdh4) "org.apache.hadoop.mapreduce.task.JobContextImpl"
        else       "org.apache.hadoop.mapreduce.JobContext")
      val jobContextConstructor = jobContextClass.getConstructor(classOf[Configuration], classOf[JobID])

      val jobClass = Class.forName("org.apache.hadoop.mapreduce.Job")
      val jobConstructor1 = jobClass.getConstructor(classOf[Configuration])
      val jobConstructor2 = jobClass.getConstructor(classOf[Configuration], classOf[String])

      val isDirectory =
        if (useCdh4) Class.forName("org.apache.hadoop.fs.FileStatus").getMethod("isDirectory")
        else Class.forName("org.apache.hadoop.fs.FileStatus").getMethod("isDir")

      val sequenceFileReaderConstructor = Class.forName("org.apache.hadoop.io.SequenceFile$Reader").getConstructor(classOf[FileSystem], classOf[Path], classOf[Configuration])

      val getConfiguration = Class.forName("org.apache.hadoop.mapreduce.JobContext").getMethod("getConfiguration")


      CDH4(useCdh4, taskAttemptContextConstructor, mapContextConstructor, jobContextConstructor,
           jobConstructor1, jobConstructor2,
           sequenceFileReaderConstructor, isDirectory, getConfiguration)
    } catch { case e: Throwable => throw new IllegalArgumentException("Error while trying to instantiate specific instances for CDH"+(if (useCdh4) "4" else "3")+": "+e.getMessage, e) }
  }

  /** @return the key to use for the default file system */
  lazy val defaultFSKeyName = if (cdh4.useCdh4) "fs.defaultFS" else "fs.default.name"

  /** @return true if the file is a directory */
  def isDirectory(fileStatus: FileStatus): Boolean = invoke(cdh4.isDirectory, fileStatus).asInstanceOf[Boolean]

  /** @return a sequence file reader */
  def newSequenceFileReader(configuration: Configuration, path: Path): SequenceFile.Reader =
    newInstance(cdh4.sequenceFileReaderConstructor, Files.fileSystem(path)(configuration), path, configuration).asInstanceOf[SequenceFile.Reader]

  /**
   * Creates TaskAttemptContext from a JobConf and jobId using the correct
   * constructor for based on the hadoop version.
   */
  def newTaskAttemptContext(conf: Configuration, id: TaskAttemptID): TaskAttemptContext =
    newInstance(cdh4.taskAttemptContextConstructor, conf, id).asInstanceOf[TaskAttemptContext]

  /**
   * Creates JobContext from a configuration and jobId using the correct
   * constructor based on the hadoop version.
   */
  def newJobContext(conf: Configuration, id: JobID): JobContext =
    newInstance(cdh4.jobContextConstructor, conf, id).asInstanceOf[JobContext]

  /**
   * Creates a new Job from a configuration and jobId using the correct
   * constructor based on the hadoop version.
   */
  def newJob(conf: Configuration): Job =
    if (useHadoop2) hadoop2.newJob(conf)
    else            newInstance(cdh4.jobConstructor1, conf).asInstanceOf[Job]

  /**
   * Creates a new Job from a configuration and jobId using the correct
   * constructor based on the hadoop version.
   */
  def newJob(conf: Configuration, name: String): Job =
    if (useHadoop2) hadoop2.newJob(conf, name)
    else            newInstance(cdh4.jobConstructor2, conf, name).asInstanceOf[Job]

  /**
   * Creates MapContext from a JobConf and jobId using the correct
   * constructor for based on the hadoop version.
   */
  def newMapContext(conf: Configuration, id: TaskAttemptID, reader: RecordReader[_,_], writer: RecordWriter[_,_], outputCommitter: OutputCommitter, reporter: StatusReporter, split: InputSplit): MapContext[Any,Any,Any,Any] =
    newInstance(cdh4.mapContextConstructor, conf, id, reader, writer, outputCommitter, reporter, split).asInstanceOf[MapContext[Any,Any,Any,Any]]

  /**
   * Rename method using the FileSystem for cdh3 and FileContext (i.e. not broken when moving directories) for cdh4 and cdh5
   */
  def rename(srcPath: Path, destPath: Path)(implicit configuration: Configuration) =
    if (cdh4.useCdh4 || useHadoop2) hadoop2.rename(srcPath, destPath)
    else                            FileSystem.get(configuration).rename(srcPath, destPath)

  /**
   * Invokes Configuration() on JobContext. Works with both
   * hadoop 1 and 2.
   */
  def getConfiguration(context: JobContext): Configuration =
    invoke(cdh4.getConfiguration, context).asInstanceOf[Configuration]

  lazy val hadoop2 = Hadoop2(HadoopDistributedCache(useHadoop2))
  lazy val useHadoop2 = try { Class.forName("org.apache.hadoop.mapreduce.filecache.DistributedCache"); logger.debug("Hadoop 2.0 compatibility class is being used"); true }
  catch { case _: Throwable => logger.debug("Hadoop < 2.0 compatibility class is being used"); false }

  case class Hadoop2(cache: HadoopDistributedCache) {
    lazy val jobGetInstanceMethod1 = getMethod("org.apache.hadoop.mapreduce.Job", "getInstance", types = Seq("org.apache.hadoop.conf.Configuration"))
    lazy val jobGetInstanceMethod2 = getMethod("org.apache.hadoop.mapreduce.Job", "getInstance", types = Seq("org.apache.hadoop.conf.Configuration", "java.lang.String"))
    lazy val getFileContextMethod  = getMethod("org.apache.hadoop.fs.FileContext", "getFileContext", types = Seq("org.apache.hadoop.conf.Configuration"))
    lazy val renameMethod          = getMethod("org.apache.hadoop.fs.FileContext", "rename", types = Seq("org.apache.hadoop.fs.Path", "org.apache.hadoop.fs.Path", "[Lorg.apache.hadoop.fs.Options$Rename;"))
    lazy val renameOptions         = Class.forName("org.apache.hadoop.fs.Options$Rename")
    // get all values, as long as OVERWRITE is in the Array, this will work
    // all other attempts to just get access to the OVERWRITE field and use it to invoke rename have failed
    lazy val overwrite = renameOptions.getDeclaredMethod("values").invoke(null)

    def newJob(conf: Configuration): Job = invokeStatic(jobGetInstanceMethod1, conf).asInstanceOf[Job]
    def newJob(conf: Configuration, name: String): Job = invokeStatic(jobGetInstanceMethod2, conf, name).asInstanceOf[Job]
    def rename(srcPath: Path, destPath: Path)(implicit configuration: Configuration) = {
      if (isDirectory(FileSystem.get(configuration).getFileStatus(srcPath))) {
        try {
          val fileContext = invokeStatic(getFileContextMethod, configuration)
          invoke(renameMethod, fileContext, srcPath, destPath,overwrite)
          true
        } catch { case e: Throwable => true }
      } else FileSystem.get(configuration).rename(srcPath, destPath)
    }
    
    private def getMethod(hadoop2Class: String, methodName: String, types: Seq[String] = Seq()) =
      Class.forName(hadoop2Class).getMethods.
        find(m => (m.getName == methodName) && (m.getParameterTypes.map(_.getName).toSeq == types)).
          getOrElse(throw new Exception(s"method $methodName not found in class $hadoop2Class"))
  }

  case class HadoopDistributedCache(useHadoop2: Boolean) {
    lazy val CACHE_FILES = if (useHadoop2) "mapreduce.job.cache.files" else "mapred.cache.files"

    lazy val addCacheFileMethod       = getCacheMethod("addCacheFile", argumentsNb = 2)
    lazy val getLocalCacheFilesMethod = getCacheMethod("getLocalCacheFiles", argumentsNb = 1)
    lazy val getCacheFilesMethod      = getCacheMethod("getCacheFiles", argumentsNb = 1)
    lazy val createSymlinkMethod      = getCacheMethod("createSymlink", argumentsNb = 1)
    lazy val addFileToClassPathMethod = getCacheMethod("addFileToClassPath", argumentsNb = 2)

    def addCacheFile(uri: URI, configuration: Configuration): Unit =
      invokeStatic(addCacheFileMethod, uri, configuration)

    def getLocalCacheFiles(configuration: Configuration): Array[Path] =
      invokeStatic(getLocalCacheFilesMethod, configuration).asInstanceOf[Array[Path]]

    def getCacheFiles(configuration: Configuration): Array[URI] =
      invokeStatic(getCacheFilesMethod, configuration).asInstanceOf[Array[URI]]

    def createSymlink(configuration: Configuration): Unit =
      invokeStatic(createSymlinkMethod, configuration)

    def addFileToClassPath(path: Path, configuration: Configuration): Unit =
      invokeStatic(addFileToClassPathMethod, path, configuration)

    private def getCacheMethod(methodName: String, argumentsNb: Int) =
      getMethod("org.apache.hadoop.mapreduce.filecache.DistributedCache",
                "org.apache.hadoop.filecache.DistributedCache", methodName, argumentsNb)

    private def getMethod(hadoop2Class: String, hadoop1Class: String, methodName: String, argumentsNb: Int = 0) =
      (if (useHadoop2) Class.forName(hadoop2Class) else Class.forName(hadoop1Class)).getMethods.
        find(m => (m.getName == methodName) && (m.getParameterTypes.toSeq.size == argumentsNb)).
        getOrElse(throw new Exception(s"method $methodName not found in class ${if (useHadoop2) hadoop2Class else hadoop1Class}"))

  }

  private def newInstance(constructor: Constructor[_], args: AnyRef*) =
    try constructor.newInstance(args:_*)
    catch { case e: Throwable => throwIllegalArgumentException(s"Can't instantiate $constructor", args, e) }

  private def invoke(method: Method, instance: AnyRef, args: AnyRef*) =
    try method.invoke(instance, args:_*)
    catch { case e: Throwable => throwIllegalArgumentException(s"Can't invoke ${method.getName}", args, e) }

  private def invokeStatic(method: Method, args: AnyRef*) =
    try method.invoke(null, args:_*)
    catch { case e: Throwable => throwIllegalArgumentException(s"Can't invoke ${method.getName}", args, e) }

  private def throwIllegalArgumentException(what: String, args: Seq[AnyRef], e: Throwable) =
    throw new IllegalArgumentException(s"$what, with arguments: ${args.mkString(", ")} -> ${e.getMessage}\n${cause(e)}")

  private def cause(e: Throwable) =
    Option(e.getCause).map(c => s"caused by ${c.getMessage}\n${c.getStackTrace.mkString("\n")}").getOrElse("")


}


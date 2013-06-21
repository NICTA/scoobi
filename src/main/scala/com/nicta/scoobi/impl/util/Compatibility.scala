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


/**
 * Provides a compatibility layer for handling API incompatibilities between
 * hadoop 1.x and 2.x via reflection.
 */
object Compatibility {
  private case class V2(useV2: Boolean,
                        taskAttemptContextConstructor: Constructor[_],
                        mapContextConstructor: Constructor[_],
                        jobContextConstructor: Constructor[_],
                        sequenceFileReaderConstructor: Constructor[_],
                        isDirectory: Method,
                        getConfiguration: Method)

  private lazy val v2: V2 = {
    // use the presence of JobContextImpl as a test for 2.x
    val useV2 = try { Class.forName("org.apache.hadoop.mapreduce.task.JobContextImpl"); true }
                catch { case _: Throwable => false }

    try {
      val taskAttemptContextClass =
        Class.forName(if (useV2) "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl"
                      else       "org.apache.hadoop.mapreduce.TaskAttemptContext")
      val taskAttemptContextConstructor = taskAttemptContextClass.getConstructor(classOf[Configuration], classOf[TaskAttemptID])

      val mapContextClass =
        Class.forName(if (useV2) "org.apache.hadoop.mapreduce.task.MapContextImpl"
        else       "org.apache.hadoop.mapreduce.MapContext")
      val mapContextConstructor = mapContextClass.getConstructor(classOf[Configuration], classOf[TaskAttemptID],
        classOf[RecordReader[_,_]], classOf[RecordWriter[_,_]],
        classOf[OutputCommitter], classOf[StatusReporter], classOf[InputSplit])

      val jobContextClass =
        Class.forName(if (useV2) "org.apache.hadoop.mapreduce.task.JobContextImpl"
        else       "org.apache.hadoop.mapreduce.JobContext")
      val jobContextConstructor = jobContextClass.getConstructor(classOf[Configuration], classOf[JobID])

      val isDirectory =
        if (useV2) Class.forName("org.apache.hadoop.fs.FileStatus").getMethod("isDirectory")
        else Class.forName("org.apache.hadoop.fs.FileStatus").getMethod("isDir")

      val sequenceFileReaderConstructor = Class.forName("org.apache.hadoop.io.SequenceFile$Reader").getConstructor(classOf[FileSystem], classOf[Path], classOf[Configuration])

      val getConfiguration = Class.forName("org.apache.hadoop.mapreduce.JobContext").getMethod("getConfiguration")


      V2(useV2, taskAttemptContextConstructor, mapContextConstructor, jobContextConstructor, sequenceFileReaderConstructor, isDirectory, getConfiguration)
    } catch { case e: Throwable => throw new IllegalArgumentException("Error while trying to instantiate specific instances for CDH"+(if (useV2) "4" else "3")+": "+e.getMessage, e) }
  }

  /** @return true if the file is a directory */
  def isDirectory(fileStatus: FileStatus): Boolean = invoke(v2.isDirectory, fileStatus).asInstanceOf[Boolean]

  /** @return a sequence file reader */
  def newSequenceFileReader(sc: ScoobiConfiguration, path: Path): SequenceFile.Reader =
    newInstance(v2.sequenceFileReaderConstructor, sc.fileSystem, path, sc.configuration).asInstanceOf[SequenceFile.Reader]

  /**
   * Creates TaskAttemptContext from a JobConf and jobId using the correct
   * constructor for based on the hadoop version.
   */
  def newTaskAttemptContext(conf: Configuration, id: TaskAttemptID): TaskAttemptContext =
    newInstance(v2.taskAttemptContextConstructor, conf, id).asInstanceOf[TaskAttemptContext]

  /**
   * Creates JobContext from a configuration and jobId using the correct
   * constructor for based on the hadoop version.
   */
  def newJobContext(conf: Configuration, id: JobID): JobContext =
    newInstance(v2.jobContextConstructor, conf, id).asInstanceOf[JobContext]

  /**
   * Creates MapContext from a JobConf and jobId using the correct
   * constructor for based on the hadoop version.
   */
  def newMapContext(conf: Configuration, id: TaskAttemptID, reader: RecordReader[_,_], writer: RecordWriter[_,_], outputCommitter: OutputCommitter, reporter: StatusReporter, split: InputSplit): MapContext[Any,Any,Any,Any] =
    newInstance(v2.mapContextConstructor, conf, id, reader, writer, outputCommitter, reporter, split).asInstanceOf[MapContext[Any,Any,Any,Any]]

  /**
   * Invokes getConfiguration() on JobContext. Works with both
   * hadoop 1 and 2.
   */
  def getConfiguration(context: JobContext): Configuration =
    invoke(v2.getConfiguration, context).asInstanceOf[Configuration]

  private def newInstance(constructor: Constructor[_], args: AnyRef*) =
    try constructor.newInstance(args:_*)
    catch { case e: Throwable => throw new IllegalArgumentException(s"Can't instantiate $constructor : ${e.getMessage}", e) }

  private def invoke(method: Method, instance: AnyRef, args: AnyRef*) =
    try method.invoke(instance, args:_*)
    catch { case e: Throwable => throw new IllegalArgumentException(s"Can't invoke ${method.getName} : ${e.getMessage}", e) }

}

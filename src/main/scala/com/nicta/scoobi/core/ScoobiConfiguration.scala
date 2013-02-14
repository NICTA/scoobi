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
package core

import java.net.URL
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapreduce.Job
import impl.ScoobiConfigurationImpl

/**
 * This class wraps the Hadoop (mutable) configuration with additional configuration information such as the jars which should be
 * added to the classpath.
 */
trait ScoobiConfiguration {
  def configuration: Configuration
  @deprecated(message="use 'configuration' instead", since="0.7.0")
  def conf: Configuration = configuration
  def userJars: Set[String]
  def userDirs: Set[String]
  def withHadoopArgs(args: Array[String])(f: Array[String] => Unit): ScoobiConfiguration
  def loadDefaults: ScoobiConfiguration
  def includeLibJars(jars: Seq[URL]): ScoobiConfiguration
  def parse(commandLineArg: String, value: String): ScoobiConfiguration
  def addJar(jar: String): ScoobiConfiguration
  def addJars(jars: Seq[String]): ScoobiConfiguration
  def addJarByClass(clazz: Class[_]): ScoobiConfiguration
  def addUserDir(dir: String): ScoobiConfiguration
  def addUserDirs(dirs: Seq[String]): ScoobiConfiguration
  def isRemote: Boolean
  def isLocal: Boolean
  def isInMemory: Boolean
  def concurrentJobs: Boolean
  def modeIs(mode: Mode.Value): ScoobiConfiguration
  def mode: Mode.Value
  def uploadedLibJars: Boolean
  def setUploadedLibJars(uploaded: Boolean)
  def setMaxReducers(maxReducers: Int)
  def getMaxReducers: Int
  def setMinReducers(minReducers: Int)
  def getMinReducers: Int
  def setBytesPerReducer(sizeInBytes: Long)
  def getBytesPerReducer: Long
  def jobNameIs(name: String)
  def jobName: Option[String]
  def jobId: String
  def jobStep(mscrId: Int): String
  def setAsInMemory: ScoobiConfiguration
  def setAsLocal: ScoobiConfiguration
  def setDirectories: ScoobiConfiguration
  def set(key: String, value: Any)
  def setScoobiDir(dir: String): ScoobiConfiguration

  def scoobiDir: String
  def workingDir: String
  def workingDirectory: Path
  def temporaryOutputDirectory(job: Job): Path
  def temporaryJarFile: File

  def deleteScoobiDirectory: Boolean
  def deleteWorkingDirectory: Boolean
  def deleteTemporaryOutputDirectory(job: Job): Boolean

  def fileSystem: FileSystem
  @deprecated(message = "use 'fileSystem' instead", since = "0.7.0")
  def fs: FileSystem = fileSystem
  def newEnv(wf: WireReaderWriter): Environment

  def persist[A](ps: Seq[Persistent[_]]): Seq[Persistent[_]]
  def persist[A](list: DList[A]): DList[A]
  def persist[A](o: DObject[A]): A
  def duplicate: ScoobiConfiguration
}

object Mode extends Enumeration {
  type Mode = Value
  val InMemory, Local, Cluster = Value
}


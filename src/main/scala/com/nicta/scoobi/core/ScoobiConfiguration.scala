package com.nicta.scoobi
package core

import java.net.URL
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import impl.Configurations._

/**
 * This class wraps the Hadoop (mutable) configuration with additional configuration information such as the jars which should be
 * added to the classpath.
 */
trait ScoobiConfiguration {
  def configuration: Configuration
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
  def fs: FileSystem
  def jobId: String
  def jobStep(mscrId: Int): String
  def conf: Configuration
  def setAsInMemory: ScoobiConfiguration
  def setAsLocal: ScoobiConfiguration
  def setDirectories: ScoobiConfiguration
  def set(key: String, value: Any)
  def setScoobiDir(dir: String): ScoobiConfiguration

  def scoobiDir: String
  def workingDir: String
  def workingDirectory: Path
  def temporaryOutputDirectory: Path
  def temporaryJarFile: File

  def deleteScoobiDirectory: Boolean
  def deleteWorkingDirectory: Boolean
  def deleteTemporaryOutputDirectory: Boolean

  def fileSystem: FileSystem

  def persist[A](ps: Seq[Persistent[_]])
  def persist[A](list: DList[A])
  def persist[A](o: DObject[A]): A

}

object Mode extends Enumeration {
  type Mode = Value
  val InMemory, Local, Cluster = Value
}
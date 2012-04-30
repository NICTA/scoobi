package com.nicta.scoobi

import impl.Configurations
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._
import scala.util.Random
import Configurations._
import com.nicta.scoobi.impl.util.JarBuilder

/**
 * This class wraps the Hadoop (mutable) configuration with additional configuration information such as the jars which should be
 * added to the classpath.
 */
case class ScoobiConfiguration(configuration: Configuration = new Configuration,
                               userJars: Set[String] = Set(),
                               userDirs: Set[String] = Set()) {

  /** Parse the generic Hadoop command line arguments, and call the user code with the remaining arguments */
  def withHadoopArgs(args: Array[String])(f: Array[String] => Unit): ScoobiConfiguration = callWithHadoopArgs(args, f)

  /** Helper method that parses the generic Hadoop command line arguments before
   * calling the user's code with the remaining arguments. */
  private def callWithHadoopArgs(args: Array[String], f: Array[String] => Unit): ScoobiConfiguration = {
    /* Parse options then update current configuration. Because the filesystem
     * property may have changed, also update working directory property. */
    val parser = new GenericOptionsParser(args)
    parser.getConfiguration.foreach { entry => configuration.set(entry.getKey, entry.getValue) }

    /* Run the user's code */
    f(parser.getRemainingArgs)
    this
  }

  def addJar(jar: String)  = copy(userJars = userJars + jar)
  def addJarByClass(clazz: Class[_])  = JarBuilder.findContainingJar(clazz).map(addJar).getOrElse(this)
  def addUserDir(dir: String)  = copy(userDirs = userDirs + withTrailingSlash(dir))

  /* Timestamp used to mark each Scoobi working directory. */
  private def timestamp = {
    val now = new Date
    val sdf = new SimpleDateFormat("yyyyMMdd-HHmmss-SSS")
    sdf.format(now)
  }

  /** we don't want some chars in hdfs path names, namely :=/\ etc */
  private val random = new Random()
  private val validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890"
  private def randomString(num: Int) = List.fill(num)(validChars(random.nextInt(validChars.size))).mkString("")

  /** The id for the current Scoobi job being (or about to be) executed. */
  lazy val jobId: String = "%s-%s-%s".format("scoobi", timestamp, randomString(16))

  /** Scoobi's configuration. */
  lazy val conf = {
    configuration.set("scoobi.jobid", jobId)
    configuration.update("scoobi.workdir", defaultWorkDir)(withTrailingSlash)
  }

  def set(key: String, value: String) { configuration.set(key, value) }

  private lazy val defaultWorkDir = withTrailingSlash(FileSystem.get(configuration).getHomeDirectory.toUri.toString)+".scoobi-tmp/"+jobId

  private def withTrailingSlash(s: String) = if (s endsWith "/") s else s + '/'

  lazy val workingDirectory: Path = new Path(conf.workingDirectory)
}

object ScoobiConfiguration {
  implicit def toExtendedConfiguration(sc: ScoobiConfiguration): ExtendedConfiguration = extendConfiguration(sc)
  implicit def toConfiguration(sc: ScoobiConfiguration): Configuration = sc.conf

  def apply(args: Array[String]): ScoobiConfiguration = ScoobiConfiguration().callWithHadoopArgs(args, (a: Array[String]) => ())
}

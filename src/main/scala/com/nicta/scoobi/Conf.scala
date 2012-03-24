package com.nicta.scoobi

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._
import Option.{apply => ?}

import com.nicta.scoobi.impl.util.JarBuilder

object Conf extends ConfTrait

trait ConfTrait {

  /** Helper method that parses the generic Haddop command line arguments before
    * calling the user's code with the remaining arguments. */
  def withHadoopArgs(args: Array[String])(f: Array[String] => Unit) = {
    /* Parse options then update current configuration. Becuase the filesystem
     * property may have changed, also update working directory property. */
    val parser = new GenericOptionsParser(args)
    parser.getConfiguration.foreach { entry => conf.set(entry.getKey, entry.getValue) }

    /* Run the user's code */
    f(parser.getRemainingArgs)
  }

  private var userJars: Set[String] = Set.empty

  /** A list of JARs required by the user for their Scoobi job. */
  def getUserJars = userJars

  /** Set a Scoobi user JAR. */
  def setJar(jar: String) = {
    userJars = userJars + jar
  }

  /** Set a Scoobi user JAR by finding an example class location. */
  def setJarByClass(clazz: Class[_]) {
    userJars = userJars ++ JarBuilder.findContainingJar(clazz)
  }

  /* Timestamp used to mark each Scoobi working directory. */
  private val timestamp = {
    val now = new Date
    val sdf = new SimpleDateFormat("yyyyMMdd-HHmmss")
    sdf.format(now)
  }

  /** The id for the current Scoobi job being (or about to be) executed. */
  val jobId: String = "scoobi-" + timestamp


  /** Scoobi's configuration. */
  val conf = {
    val c = new Configuration
    c.set("scoobi.jobid", jobId)
    c
  }

  private def withTrailingSlash(s: String) = if (s endsWith "/") s else s + '/'

  def getJobId(conf: Configuration): String =
    ?(conf.get("scoobi.jobid")).getOrElse(sys.error("Scoobi job id not set."))

  /** Get the Scoobi working directory. */
  def getWorkingDirectory(conf: Configuration): Path = new Path(
    (?(conf.get("scoobi.workdir")) match {
      case Some(s) => withTrailingSlash(s)
      case None    => withTrailingSlash(FileSystem.get(conf).getHomeDirectory.toUri.toString) + ".scoobi-tmp/"
     }) + getJobId(conf))
}

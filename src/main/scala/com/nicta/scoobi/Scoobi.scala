/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import Option.{apply => ?}


/** Global Scoobi functions and values. */
object Scoobi {

  private var userJars: Set[String] = Set.empty

  /** A list of JARs required by the user for their Scoobi job. */
  def getUserJars = userJars

  /** Set a Scoobi user JAR. */
  def setJar(jar: String) = {
    userJars = userJars + jar
  }

  /** Set a Scoobi user JAR by finding an example class location. */
  def setJarByClass(clazz: Class[_]) = {
    userJars = userJars  + JarBuilder.findContainingJar(clazz)
  }

  /* Timestamp used to mark each Scoobi working directory. */
  private val timestamp = {
    val now = new Date
    val sdf = new SimpleDateFormat("yyyyMMddHHmm")
    sdf.format(now)
  }

  /** The id for the current Scoobi job being (or about to be) executed. */
  val jobId: String = timestamp


  /** Scoobi's configuration. */
  val conf: JobConf = {
    val jc = new JobConf(new Configuration)

    /* The Scoobi working directory is in the user's home directory under '.scoobi' and
     * a timestamped directory. e.g. /home/fred/.scoobi/201110041326. */
    val workdirPath = new Path(FileSystem.get(jc).getHomeDirectory, ".scoobi/" + jobId)
    val workdir = workdirPath.toUri.toString
    jc.set("scoobi.workdir", workdir)

    /* Scoobi job name */
    jc.setJobName("scoobi_" + jobId)

    jc
  }

  /** Get the Scoobi working directory. */
  def getWorkingDirectory(jobConf: JobConf): Path = {
    ?(jobConf.get("scoobi.workdir")) match {
      case Some(s)  => new Path(s)
      case None     => error("Scoobi working directory not set.")
    }
  }
}

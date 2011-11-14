/**
  * Copyright 2011 National ICT Australia Limited
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

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import scala.collection.JavaConversions._
import Option.{apply => ?}

import com.nicta.scoobi.impl.util.JarBuilder


/** Global Scoobi functions and values. */
object Scoobi {

  /** Helper method that parses the generic Haddop command line arguments before
    * calling the user's code with the remaining arguments. */
  def withHadoopArgs(args: Array[String])(f: Array[String] => Unit) = {
    /* Parse options then update current configuration. Becuase the filesystem
     * property may have changed, also update working directory property. */
    val parser = new GenericOptionsParser(args)
    parser.getConfiguration.foreach { entry => internalConf.set(entry.getKey, entry.getValue) }

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
  lazy val conf: JobConf = {
    setWorkingDirectory(internalConf)
    val job = new JobConf(internalConf)
    job.setJobName("scoobi_" + jobId)
    job
  }

  /** Scoobi's configuration. */
  private val internalConf = new Configuration

  /* The Scoobi working directory is in the user's home directory under '.scoobi' and
   * a timestamped directory. e.g. /home/fred/.scoobi/201110041326. */
  private def setWorkingDirectory(jobConf: Configuration) = {
    val workdirPath = new Path(FileSystem.get(jobConf).getHomeDirectory, ".scoobi/" + jobId)
    val workdir = workdirPath.toUri.toString
    jobConf.set("scoobi.workdir", workdir)
  }

  /** Get the Scoobi working directory. */
  def getWorkingDirectory(jobConf: Configuration): Path = {
    ?(jobConf.get("scoobi.workdir")) match {
      case Some(s)  => new Path(s)
      case None     => sys.error("Scoobi working directory not set.")
    }
  }
}

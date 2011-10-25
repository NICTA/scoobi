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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import Option.{apply => ?}

import com.nicta.scoobi.impl.util.JarBuilder


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
      case None     => sys.error("Scoobi working directory not set.")
    }
  }
}

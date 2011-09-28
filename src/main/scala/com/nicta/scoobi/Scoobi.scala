/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf


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

  /** Get the Scoobi working directory. */
  def getWorkingDirectory(conf: JobConf): Path = new Path(conf.getWorkingDirectory, ".scoobi")
}

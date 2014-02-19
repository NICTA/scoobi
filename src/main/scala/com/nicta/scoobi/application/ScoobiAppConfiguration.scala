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
package application

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory
import sys.process._
import impl.monitor.Loggable._
import java.io.File
import impl.control.SystemProperties

import core._
import impl.ScoobiConfiguration
import core.ScoobiConfiguration

/**
 * This trait provides a ScoobiConfiguration object initialised with the configuration files found in the
 * $HADOOP_HOME/conf directory.
 *
 * If the ScoobiArgs indicates a local execution the configuration files are not added to the Hadoop configuration
 * object because it wouldn't be possible to remove them afterwards.
 *
 */
trait ScoobiAppConfiguration extends ClusterConfiguration with ScoobiArgs with SystemProperties {
  private implicit lazy val logger = LogFactory.getLog("scoobi.ScoobiAppConfiguration")

  lazy val HADOOP_HOME     = getEnv("HADOOP_HOME").orElse(get("HADOOP_HOME"))
  lazy val HADOOP_CONF_DIR = getEnv("HADOOP_CONF_DIR").orElse(get("HADOOP_CONF_DIR"))

  lazy val HADOOP_COMMAND = "which hadoop".lines_!.headOption
  lazy val hadoopHomeDir =
    HADOOP_HOME.map(_.debug("got the hadoop directory from the $HADOOP_HOME variable")).
      orElse(HADOOP_COMMAND.map(_.replaceAll("/bin/hadoop$", "").debug("got the hadoop directory from the hadoop executable")))

  lazy val hadoopConfDirs: Seq[String] =
    HADOOP_CONF_DIR.map(dir => Seq(dir+"/")).
      orElse(hadoopHomeDir.map(homeDir => Seq("conf", "etc").map(d => homeDir+"/"+d+"/"))).toSeq.flatten

  /** default configuration */
  implicit def configuration: ScoobiConfiguration = ScoobiConfiguration(setConfiguration(new Configuration))

  /** set the default configuration, depending on the arguments */
  def setConfiguration(c: Configuration) =  {
    if (useHadoopConfDir) configurationFromConfigurationDirectory(c)
    else                  c
  }

  /** @return true if there are some configuration directories for hadoop */
  def isHadoopConfigured = hadoopConfDirs.nonEmpty

  lazy val configurationFilesDiagnostic = {
    if (!isHadoopConfigured) logger.error(s"No configuration directory could be found. $$HADOOP_HOME is $HADOOP_HOME, $$HADOOP_CONF_DIR is $HADOOP_CONF_DIR")
    hadoopConfDirs.foreach { dir =>
      logger.info(s"looking for configuration files in $dir. Found ${configurationFiles.map(name => s"$name: ${new File(dir+"/"+name).exists}").mkString(", ")}")
    }
  }

  /** @return a configuration object set with the properties found in the hadoop directories */
  def configurationFromConfigurationDirectory(conf: Configuration) = {
    conf.clear
    configurationFilesDiagnostic

    hadoopConfDirs.foreach { dir =>
      configurationFiles.foreach { r =>
        val path = dir+r
        if (new File(path).exists) {
          conf.addResource(new Path(path).debug("adding the properties file:"))
        }
      }
    }
    conf
  }

  private val configurationFiles = Seq("core-site.xml", "mapred-site.xml", "hdfs-site.xml", "yarn-site.xml")
}


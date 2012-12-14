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

  protected lazy val HADOOP_HOME =
    getEnv("HADOOP_HOME").
      orElse(get("HADOOP_HOME")).
      getOrElse(throw new Exception("The HADOOP_HOME variable is must be set to access the configuration files"))

  protected lazy val HADOOP_COMMAND = "which hadoop".lines_!.headOption

  /** default configuration */
  implicit def configuration: ScoobiConfiguration = {
    if (useHadoopConfDir) ScoobiConfiguration(configurationFromConfigurationDirectory)
    else                  ScoobiConfiguration()
  }

  def configurationFromConfigurationDirectory = {
    val hadoopHomeDir =
      HADOOP_COMMAND.map(_.replaceAll("/bin/hadoop$", "").debug("got the hadoop directory from the hadoop executable")).
        getOrElse(HADOOP_HOME.map(_.debug("got the hadoop directory from the $HADOOP_HOME variable")))

    val conf = new Configuration
    Seq("conf", "etc").map(d => hadoopHomeDir+"/"+d+"/").foreach { dir =>
      Seq("core-site.xml", "mapred-site.xml", "hdfs-site.xml").foreach { r =>
        if (new File(dir+r).exists) {
          conf.addResource(new Path(dir+r).debug("adding the properties file:"))
        }
      }
    }
    conf
  }
}
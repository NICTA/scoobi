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

/**
 * This trait provides a ScoobiConfiguration object initialized with the configuration files found in the
 * $HADOOP_HOME/conf directory.
 *
 * If the ScoobiArgs indicates a local execution the configuration files are not added to the Hadoop configuration
 * object because it wouldn't be possible to remove them afterwards.
 *
 */
trait ScoobiAppConfiguration extends ClusterConfiguration with ScoobiArgs {
  private implicit lazy val logger = LogFactory.getLog("scoobi.ScoobiAppConfiguration")

  lazy val HADOOP_HOME = sys.env.get("HADOOP_HOME").orElse(sys.props.get("HADOOP_HOME"))

  /** default configuration */
  implicit def configuration: ScoobiConfiguration = {
    if (useHadoopConfDir) ScoobiConfiguration(configurationFromConfigurationDirectory)
    else                  ScoobiConfiguration()
  }

  def configurationFromConfigurationDirectory = {

    val hadoopHomeDir = "which hadoop".lines_!.headOption.map(_.replaceAll("/bin/hadoop$", "").debug("got the hadoop directory from the hadoop executable")).orElse(
                        HADOOP_HOME.map(_.debug("got the hadoop directory from the $HADOOP_HOME variable")))

    val conf = new Configuration
    Seq("conf", "etc").map(d => hadoopHomeDir+"/"+d+"/").foreach { dir =>
      Seq("core-site.xml", "mapred-site.xml", "hdfs-site.xml").foreach { r =>
        val path = new Path(dir+r)
        if (new File(dir+r).exists) {
          logger.debug("adding the properties file: "+path)
          conf.addResource(path)
        }
      }
    }
    conf
  }
}
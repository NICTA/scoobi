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

/**
 * This trait provides a ScoobiConfiguration object initialized with the configuration files found in the
 * $HADOOP_HOME/conf directory.
 *
 * If the ScoobiArgs indicates a local execution the configuration files are not added to the Hadoop configuration
 * object because it wouldn't be possible to remove them afterwards.
 *
 */
trait ScoobiAppConfiguration extends ClusterConfiguration with ScoobiArgs {

  lazy val HADOOP_HOME = sys.env.get("HADOOP_HOME").orElse(sys.props.get("HADOOP_HOME"))

  lazy val HADOOP_CONF_DIR = HADOOP_HOME.map(_+"/conf/")

  /** default configuration */
  implicit def configuration: ScoobiConfiguration = {
    if (useHadoopConfDir) ScoobiConfiguration(configurationFromConfigurationDirectory)
    else                  ScoobiConfiguration()
  }

  def configurationFromConfigurationDirectory = {
    HADOOP_CONF_DIR.map { dir =>
      val conf = new Configuration
      conf.addResource(new Path(dir+"core-site.xml"))
      conf.addResource(new Path(dir+"mapred-site.xml"))
      conf.addResource(new Path(dir+"hdfs-site.xml"))
      conf
    }.getOrElse(new Configuration)
  }
}
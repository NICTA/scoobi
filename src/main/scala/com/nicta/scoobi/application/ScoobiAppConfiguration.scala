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

  private lazy val HADOOP_HOME = sys.env.get("HADOOP_HOME")

  private lazy val HADOOP_CONF_DIR = HADOOP_HOME.map(_+"/conf/")

  /** default configuration */
  implicit lazy val configuration: ScoobiConfiguration = {
    if (locally) ScoobiConfiguration()
    else         ScoobiConfiguration(configurationFromConfigurationDirectory)
  }

  def configurationFromConfigurationDirectory = {
    HADOOP_CONF_DIR.map { dir =>
      val conf = new Configuration
      conf.addResource(new Path(dir+"core-site.xml"))
      conf.addResource(new Path(dir+"mapred-site.xml"))
      conf
    }.getOrElse(new Configuration)
  }
}
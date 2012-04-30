package com.nicta.scoobi.testing

import org.apache.hadoop.fs.FileSystem
import com.nicta.scoobi.ScoobiConfiguration

/**
 * Definition of the Cluster addresses
 */
trait Cluster {
  /** @return the filesystem address  */
  def fs: String
  /** @return the jobtracker address  */
  def jobTracker: String
}

/**
 * Implementation of the Cluster trait taking the configuration from the HADOOP_HOME directory
 */
trait HadoopHomeDefinedCluster extends Cluster {
  private lazy val HADOOP_HOME =
    sys.props.get("HADOOP_HOME").getOrElse(sys.error("HADOOP_HOME must be set if you're using the "+getClass.getSimpleName+" trait"))

  private lazy val CONFIGURATION = ScoobiConfiguration(Array[String]()).configuration

  def fs = Option(CONFIGURATION.get(FileSystem.FS_DEFAULT_NAME_KEY)).getOrElse(
             sys.error("the "+FileSystem.FS_DEFAULT_NAME_KEY+" property should be set in the $HADOOP_HOME/conf/core-site.xml file"))

  def jobTracker = Option(CONFIGURATION.get("mapred.job.tracker")).getOrElse(
    sys.error("the mapred.job.tracker property should be set in the $HADOOP_HOME/conf/mapred-site.xml file"))
}
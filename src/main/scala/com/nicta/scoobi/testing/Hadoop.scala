package com.nicta.scoobi
package testing

import application._

/**
 * This trait provides methods to execute map-reduce code, either locally or on the cluster.
 *
 * @see WithLocalHadoop
 *
 * To use this trait, you need to define:
 *
 *  - the file system address: def fs = "hdfs://svm-hadoop1.ssrg.nicta.com.au"
 *  - the job tracker address: def jobTracker = "svm-hadoop1.ssrg.nicta.com.au:8021"
 */

trait Hadoop extends LocalHadoop with ClusterConfiguration with LibJars {

  /** @return true if you want to include the library jars in the jar that is sent to the cluster for each job */
  def includeLibJars = false

  /** @return the classes directories to include on a job classpath */
  def classDirs: Seq[String] = Seq("classes", "test-classes").map("target/scala-"+util.Properties.releaseVersion.getOrElse("2.9.1")+"/"+_)

  /** execute some code on the cluster, possibly showing the execution time */
  def onCluster[T](t: =>T) = showTime(executeOnCluster(t))(displayTime("Cluster execution time"))

  /**
   * execute some code on the cluster, setting the filesystem / jobtracker addresses and setting up the classpath
   */
  def executeOnCluster[T](t: =>T)(implicit configuration: ScoobiConfiguration = ScoobiConfiguration()) = {
    configureForCluster
    runOnCluster(t)
  }

  /**
   * @return a configuration with cluster setup
   */
  def configureForCluster(implicit configuration: ScoobiConfiguration): ScoobiConfiguration = {
    setLogFactory()
    configuration.jobNameIs(getClass.getSimpleName)
    configuration.setRemote
    configuration.set("fs.default.name", fs)
    configuration.set("mapred.job.tracker", jobTracker)
    if (includeLibJars) configuration.includeLibJars(jars)

    configureJars
    configuration.addUserDirs(classDirs)
  }

  /**
   * @return the cluster evaluation of t
   */
  def runOnCluster[T](t: =>T) = t
}

/**
 * This trait abstracts the creation of the ScoobiConfiguration for the Cluster
 */
trait ClusterConfiguration extends Cluster {
  /**
   * @return a cluster configuration
   */
  def clusterConfiguration = configureForCluster(new ScoobiConfiguration)

  /**
   * @return a configuration with cluster setup
   */
  def configureForCluster(implicit configuration: ScoobiConfiguration): ScoobiConfiguration

}
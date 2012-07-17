package com.nicta.scoobi
package application

/**
 * This trait provides methods to execute map-reduce code, either locally or on the cluster.
 *
 * @see LocalHadoop
 *
 * To use this trait, you need to provide:
 *
 *  - the file system address: def fs = "hdfs://svm-hadoop1.ssrg.nicta.com.au"
 *  - the job tracker address: def jobTracker = "svm-hadoop1.ssrg.nicta.com.au:8021"
 */
trait Hadoop extends LocalHadoop with Cluster with LibJars {

  /** @return true if you want to include the library jars in the jar that is sent to the cluster for each job */
  def includeLibJars = false

  /** @return the classes directories to include on a job classpath */
  def classDirs: Seq[String] = Seq("classes", "test-classes").map("target/scala-"+util.Properties.releaseVersion.getOrElse("2.9.2")+"/"+_)

  /** execute some code on the cluster, possibly showing the execution time */
  def onCluster[T](t: =>T)(implicit configuration: ScoobiConfiguration) =
    showTime(executeOnCluster(t))(displayTime("Cluster execution time"))

  /** execute some code, either locally or on the cluster, depending on the local argument being passed on the commandline */
  def onHadoop[T](t: =>T)(implicit configuration: ScoobiConfiguration) =
    if (locally) onLocal(t) else onCluster(t)

  /**
   * execute some code on the cluster, setting the filesystem / jobtracker addresses and setting up the classpath
   */
  def executeOnCluster[T](t: =>T)(implicit configuration: ScoobiConfiguration) = {
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
    // delete libjars on the cluster
    if (deleteLibJars) deleteJars
    // include libjars in the ScoobiJob jar
    if (includeLibJars) configuration.includeLibJars(jars)

    configureJars
    configuration.addUserDirs(classDirs)
  }

  /**
   * @return the cluster evaluation of t
   */
  def runOnCluster[T](t: =>T) = t

}

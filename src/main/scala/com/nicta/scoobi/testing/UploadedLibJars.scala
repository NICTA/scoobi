package com.nicta.scoobi.testing

import org.specs2.specification._

/**
 * This trait can be mixed in a Specification to automatically add a setup step uploading the library jars to the cluster
 */
trait UploadedLibJars extends LibJars with SpecificationStructure with HadoopExamples {

  /**
   * add a first step to upload the library jars before doing anything else
   */
  override def map(fs: =>Fragments) = fs.insert(uploadStep)

  /** create a Step to upload the jars on the cluster */
  def uploadStep = Step(if (executeRemotely && context.isRemote) uploadLibJars(clusterConfiguration))

  /** @return true if the examples need to be executed remotely */
  private def executeRemotely = arguments.keep("cluster") ||  arguments.keep("hadoop")
}
package com.nicta.scoobi
package application

/**
 * This trait can be extended to create an application running Scoobi code.
 *
 * Command-line arguments are available in the args attribute (minus the hadoop specific ones) and a default
 * implicit ScoobiConfiguration is also accessible to create DLists.
 *
 * Before the code is executed the dependent jars loaded in this class classloader will be uploaded to the cluster
 * (if not already there, @see LibJars for the details).
 *
 * This behavior can be switched off by overriding the `upload` method: `override def upload = false`
 */
trait ScoobiApp extends LibJars {

  /** command-line arguments */
  protected var args: Array[String] = Array()

  /** default configuration */
  protected implicit val configuration: ScoobiConfiguration = ScoobiConfiguration()

  /** this method needs to be overridden and define the code to be executed */
  def run()

  /**
   * parse the command-line argument and:
   *
   *  - upload the dependent jars on the cluster
   *
   *  - execute the user code
   */
  def main(arguments: Array[String]) {
    configuration.withHadoopArgs(arguments) { remainingArgs =>
      args = remainingArgs
      uploadLibJars
      run()
    }
  }

}

package com.nicta.scoobi
package application

/**
 * This trait can be mixed in an Application trait in order to store command-line arguments before any code is executed
 */
trait ScoobiCommandLineArgs extends DelayedInit {

  var commandLineArguments = Seq[String]()
  private var body: () => Unit = () => ()

  /**
   * set the command line arguments and trigger the body
   */
  def set(args: Seq[String]) {
    commandLineArguments = args
    body()
  }

  def delayedInit(x: =>Unit) { body = () => x }
}

package com.nicta.scoobi
package application

/**
 * This trait can be mixed in an Application trait in order to store command-line arguments before any code is executed
 */
trait ScoobiCommandLineArgs extends DelayedInit {

  protected[scoobi] var commandLineArguments = Seq[String]()
  protected[scoobi] var scoobiArguments = Seq[String]()
  protected[scoobi] var userArguments = Seq[String]()

  private var body: () => Unit = () => ()

  /**
   * set the command line arguments and trigger the body
   */
  private[scoobi] def set(args: Seq[String]) {
    commandLineArguments = args
    setScoobiArgs(args)
    body()
  }

  private[scoobi] def setScoobiArgs(args: Seq[String]) {
    val after = args.dropWhile(!_.toLowerCase.startsWith("scoobi"))
    scoobiArguments = after.drop(1).take(1).map(_.toLowerCase).flatMap(_.split("\\."))
  }

  private[scoobi] def setRemainingArgs(args: Seq[String]) {
    val (before, after) = args.span(!_.toLowerCase.startsWith("scoobi"))
    userArguments = before ++ after.drop(2)
  }

  def delayedInit(x: =>Unit) { body = () => x }
}

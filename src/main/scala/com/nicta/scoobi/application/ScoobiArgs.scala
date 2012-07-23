package com.nicta.scoobi
package application

import HadoopLogFactory._
/**
 * This trait defines all the options which can be used to modify the behavior of a Scoobi application
 */
trait ScoobiArgs {
  /** @return true to suppress log messages */
  def quiet = true
  /** @return true to display execution times for each job */
  def showTimes = false
  /** @return the log level to use when logging */
  def level = INFO
  /** @return the categories to show when logging, as a regular expression */
  def categories = ".*"
  /** @return true if the Scoobi job must be run locally */
  def locally = false
  /** @return true if the libjars must be deleted before the Scoobi job runs */
  def deleteLibJars = false
  /** @return false if temporary files and working directory must be cleaned-up after job execution */
  def keepFiles = false
}

/**
 * Implementation of the ScoobiArgs trait taking the values from the command line arguments
 */
trait ScoobiUserArgs extends ScoobiArgs {
  /** scoobi arguments passed on the command-line, i.e. values after 'scoobi' */
  def scoobiArgs: Seq[String]

  override def showTimes     = matches(".*.times.*")  || super.showTimes
  override def quiet         = !verboseArg.isDefined  && super.quiet
  override def level         = extractLevel(verboseArg.getOrElse(""))
  override def categories    = extractCategories(verboseArg.getOrElse(""))
  override def locally       = is("local")
  override def deleteLibJars = is("deletelibjars")
  override def keepFiles     = is("keepfiles")

  private def is(argName: String)      = argumentsValues.exists(_.contains(argName))
  private def matches(argName: String) = argumentsValues.exists(_.matches(argName))

  private[scoobi]
  lazy val argumentsValues = scoobiArgs

  private[scoobi]
  lazy val verboseArg = argumentsValues.find(_.matches(".*verbose.*"))

  private[scoobi]
  def verboseDetails(args: String) = args.split("\\.").filterNot(Seq("verbose", "times").contains)

  private[scoobi]
  def extractLevel(args: String) =
    verboseDetails(args).map(l => l.toUpperCase.asInstanceOf[Level]).headOption.getOrElse(INFO)

  /**
   * extract the categories as a regular expression from the scoobi arguments, once all the other argument names have been
   * removed.
   *
   * While this not strictly necessary right now the categories regular expression can be enclosed in `[]` to facilitate
   * reading the options
   */
  private[scoobi]
  def extractCategories(args: String) = {
    val extracted = verboseDetails(args).filterNot(a => allLevels contains a.toUpperCase).mkString(".").replace("[", "").replace("]", "")
    if (extracted.isEmpty) ".*" else extracted
  }
}

package com.nicta.scoobi
package application

import HadoopLogFactory._
import org.specs2.main.CommandLineArguments

/**
 * This trait defines all the options which can be used to modify the behavior of a Scoobi application
 */
trait ScoobiArgs {
  /** @return true to suppress log messages */
  def quiet = false
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

  override def showTimes     = matches(".*.times.*")
  override def quiet         = isQuiet
  override def level         = extractLevel(argumentsValues)
  override def categories    = extractCategories(argumentsValues)
  override def locally       = is("local")
  override def deleteLibJars = is("deletelibjars")
  override def keepFiles     = is("keepfiles")

  private def is(argName: String)      = argumentsValues.exists(_.contains(argName))
  private def matches(argName: String) = argumentsValues.exists(_.matches(argName))

  private[scoobi]
  lazy val argumentsValues = scoobiArgs

  private[scoobi]
  lazy val argumentsNames = Seq("times", "local", "deletelibjars", "keepfiles", "quiet", "verbose", "cluster")

  private[scoobi]
  lazy val isVerbose = argumentsValues.exists(_ == "verbose")

  private[scoobi]
  lazy val isQuiet = argumentsValues.exists(_ == "quiet")

  private[scoobi]
  def extractLevel(args: Seq[String]): Level =
    args.filter(a => allLevels contains a.toUpperCase).map(l => l.toUpperCase.asInstanceOf[Level]).headOption.getOrElse(INFO)

  /**
   * extract the categories as a regular expression from the scoobi arguments, once all the other argument names have been
   * removed.
   *
   * While this not strictly necessary right now the categories regular expression can be enclosed in `[]` to facilitate
   * reading the options
   */
  private[scoobi]
  def extractCategories(args: Seq[String]): String = {
    val extracted = args.filterNot(argumentsNames.contains).filterNot(a => allLevels contains a.toUpperCase).mkString(".").replace("[", "").replace("]", "")
    if (extracted.isEmpty) ".*" else extracted
  }
  /** testing method */
  private[scoobi]
  def extractCategories(args: String): String = extractCategories(args.split("\\."))

  /** testing method */
  private[scoobi]
  def extractLevel(args: String): Level = extractLevel(args.split("\\."))
}

trait CommandLineScoobiUserArgs extends ScoobiArgs with CommandLineArguments {
  /** the scoobi arguments passed on the command line */
  lazy val scoobiArgs = arguments.commandLine.arguments.dropWhile(a => a != "scoobi").drop(1).flatMap(_.split("\\."))

}
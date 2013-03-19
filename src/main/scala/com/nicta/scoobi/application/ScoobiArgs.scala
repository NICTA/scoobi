/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package application

import HadoopLogFactory._
import org.specs2.main.CommandLineArguments
import Levels._
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
  /** @return true if cluster configuration must be loaded from Hadoop's configuration directory */
  def useHadoopConfDir = false
  /** @return true if the libjars must be deleted before the Scoobi job runs */
  def deleteLibJars = false
  /** @return false if libjars are used */
  def noLibJars = false
  /** @return false if temporary files and working directory must be cleaned-up after job execution */
  def keepFiles = false
}

/**
 * Implementation of the ScoobiArgs trait taking the values from the command line arguments
 */
trait ScoobiUserArgs extends ScoobiArgs {
  /** scoobi arguments passed on the command-line, i.e. values after 'scoobi' */
  def scoobiArgs: Seq[String]

  override def showTimes        = is("times")
  override def quiet            = isQuiet
  override def level            = extractLevel(argumentsValues)
  override def categories       = extractCategories(argumentsValues)
  override def locally          = (isLocal || isInMemory) && !isCluster
  override def useHadoopConfDir = is("useconfdir")
  override def deleteLibJars    = is("deletelibjars")
  override def noLibJars        = is("nolibjars")
  override def keepFiles        = is("keepfiles")

  def isInMemory                = is("inmemory")
  def isLocal                   = is("local")
  def isCluster                 = is("cluster")

  /** @return true if the cluster argument is specified and the local argument is not */
  def isClusterOnly             = isCluster && !isLocal
  /** alias for locally */
  def isLocalOnly               = locally

  private[scoobi] def is(argName: String)      = argumentsValues.exists(_.contains(argName))
  private[scoobi] def matches(argName: String) = argumentsValues.exists(_.matches(argName))

  private[scoobi]
  def argumentsValues = scoobiArgs

  private[scoobi]
  lazy val argumentsNames = Seq("times", "local", "!local", "useconfdir", "deletelibjars", "nolibjars",
                                "keepfiles", "quiet", "verbose", "cluster", "!cluster", "inmemory", "!inmemory")

  private[scoobi]
  def isVerbose = argumentsValues.exists(_ == "verbose")

  private[scoobi]
  def isQuiet = argumentsValues.exists(_ == "quiet")

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

trait CommandLineScoobiUserArgs extends ScoobiUserArgs with CommandLineArguments {
  /** the scoobi arguments passed on the command line */
  lazy val scoobiArgs = {
    val args = arguments.commandLine.arguments.dropWhile(a => a != "scoobi").drop(1).flatMap(_.split("\\."))
    if (args.isEmpty) sys.props.get("scoobi").map(_.split("\\.").toSeq).getOrElse(Seq())
    else              args
  }

}
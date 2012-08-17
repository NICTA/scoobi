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

  /**
   * scoobi arguments are the ones which are dot-separated after 'scoobi'
   *
   * hadoop jar job.jar file.txt scoobi verbose.all file2.txt
   * => scoobiArguments = Seq(verbose, all)
   */
  private[scoobi] def setScoobiArgs(args: Seq[String]) {
    val after = args.dropWhile(!_.toLowerCase.startsWith("scoobi"))
    scoobiArguments = after.drop(1).take(1).map(_.toLowerCase).flatMap(_.split("\\."))
  }

  /**
   * the users arguments are the ones which are before and after the scoobi arguments:
   *
   * hadoop jar job.jar file.txt scoobi verbose.all file2.txt
   * => userArguments = Seq(file.txt, file2.txt)
   */
  private[scoobi] def setRemainingArgs(args: Seq[String]) {
    val (before, after) = args.span(!_.toLowerCase.startsWith("scoobi"))
    userArguments = before ++ after.drop(2)
  }

  def delayedInit(x: =>Unit) { body = () => x }
}

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

import org.apache.commons.logging.LogFactory
import impl.time.SimpleTimer
import HadoopLogFactory._
import org.apache.hadoop.fs.FileSystem._

/**
 * Execute Hadoop code locally
 */
trait LocalHadoop extends ScoobiUserArgs {

  def hadoopArgs = Array[String]()

  /**
   * Static setup to use a testing log factory
   */
  def setLogFactory(name: String = classOf[HadoopLogFactory].getName) {
    HadoopLogFactory.setLogFactory(name, quiet, showTimes, level, categories)
  }

  /** execute some code locally, possibly showing execution times */
  def onLocal[T](t: =>T)(implicit configuration: ScoobiConfiguration) =
    showTime(executeOnLocal(t))(displayTime("Local execution time"))

  /** execute some code locally */
  def executeOnLocal[T](t: =>T)(implicit configuration: ScoobiConfiguration) = {
    setLogFactory()
    configureForLocal
    runOnLocal(t)
  }

  /**
   * @return the result of the local run
   */
  def runOnLocal[T](t: =>T) = t

  /**
   * @return a configuration with local setup
   */
  def configureForLocal(implicit configuration: ScoobiConfiguration): ScoobiConfiguration =
    configuration.setAsLocal

  /** @return a function to display execution times. The default uses log messages */
  def displayTime(prefix: String) = (timer: SimpleTimer) => {
    LogFactory.getFactory.getInstance(SCOOBI_TIMES).info(prefix+": "+timer.time)
    ()
  }

  /**
   * @return the time for the execution of a piece of code
   */
  def withTimer[T](t: =>T): (T, SimpleTimer) = {
    val timer = (new SimpleTimer).start
    val result = t
    (result, timer.stop)
  }

  /**
   * measure the time taken by some executed code and display the time with a specific display function
   */
  def showTime[T](t: =>T)(display: SimpleTimer => scala.Unit): T = {
    val (result, timer) = withTimer(t)
    display(timer)
    result
  }

}

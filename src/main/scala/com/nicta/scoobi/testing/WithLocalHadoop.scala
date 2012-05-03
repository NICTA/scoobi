package com.nicta.scoobi.testing

import org.apache.commons.logging.LogFactory
import org.specs2.time.SimpleTimer
import WithHadoopLogFactory._
import com.nicta.scoobi.ScoobiConfiguration

/**
 * Execute Hadoop code locally
 */
trait WithLocalHadoop {
  def hadoopArgs = Array[String]()

  /** @return true to suppress log messages */
  def quiet = true
  /** @return true to display execution times for each job */
  def showTimes = false

  /**
   * Static setup to use a testing log factory
   */
  def setLogFactory(name: String = classOf[WithHadoopLogFactory].getName) {
    WithHadoopLogFactory.setLogFactory(name, quiet, showTimes)
  }

  /** execute some code locally, possibly showing execution times */
  def onLocal[T](t: =>T) = showTime(executeOnLocal(t))(displayTime("Local execution time"))

  /** execute some code locally */
  def executeOnLocal[T](t: =>T)(implicit configuration: ScoobiConfiguration = ScoobiConfiguration()) = {
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
  def configureForLocal(implicit configuration: ScoobiConfiguration): ScoobiConfiguration = {
    setLogFactory()
    configuration
  }

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
  def showTime[T](t: =>T)(display: SimpleTimer => Unit): T = {
    val (result, timer) = withTimer(t)
    display(timer)
    result
  }

}

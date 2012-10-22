package com.nicta.scoobi
package application

import impl.time.SimpleTimer
import org.apache.commons.logging.LogFactory
import HadoopLogFactory._
import org.apache.hadoop.fs.FileSystem._
import Mode._

trait InMemoryHadoop extends ScoobiUserArgs {

  def hadoopArgs = Array[String]()

  /** execute some code in memory, using a collection backend, possibly showing execution times */
  def inMemory[T](t: =>T)(implicit configuration: ScoobiConfiguration) =
    showTime(executeInMemory(t))(displayTime("InMemory execution time"))
  
  /** execute some code locally */
  def executeInMemory[T](t: =>T)(implicit configuration: ScoobiConfiguration) = {
    setLogFactory()
    configureForInMemory
    runInMemory(t)
  }
  
  /**
   * @return the result of the in-memory run
   */
  def runInMemory[T](t: =>T) = t
  
  /**
   * @return a configuration with memory setup
   */
  def configureForInMemory(implicit configuration: ScoobiConfiguration): ScoobiConfiguration = {
    configuration.modeIs(InMemory)
    configuration.setAsInMemory    
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
  def showTime[T](t: =>T)(display: SimpleTimer => scala.Unit): T = {
    val (result, timer) = withTimer(t)
    display(timer)
    result
  }
  
  /**
   * Static setup to use a testing log factory
   */
  def setLogFactory(name: String = classOf[HadoopLogFactory].getName) {
    HadoopLogFactory.setLogFactory(name, quiet, showTimes, level, categories)
  }

}

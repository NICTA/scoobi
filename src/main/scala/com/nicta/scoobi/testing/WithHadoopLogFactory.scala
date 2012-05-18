package com.nicta.scoobi.testing

import org.apache.commons.logging.LogFactory
import java.lang.Class
import org.apache.commons.logging.impl.{SimpleLog, NoOpLog, LogFactoryImpl}
import WithHadoopLogFactory._
import java.util.logging.Level

/**
 * Log factory used for testing.
 *
 * It doesn't display any log message by default, unless the QUIET attribute is set to true.
 *
 * It can display SCOOBI_TIMES messages if the SHOW_TIMES attributes is true
 */
class WithHadoopLogFactory() extends LogFactory {

  def quiet     = Option(getAttribute(QUIET)).map(_.asInstanceOf[Boolean]).getOrElse(true)
  def showTimes = Option(getAttribute(SHOW_TIMES)).map(_.asInstanceOf[Boolean]).getOrElse(false)
  def logLevel  = Option(getAttribute(LOG_LEVEL)).map(_.asInstanceOf[Level]).getOrElse(Level.INFO)

  private val impl   = new LogFactoryImpl
  private val noOps  = new NoOpLog
  private def simple = {
    val log = new SimpleLog("TESTING")
    log.setLevel(commonsLevel(logLevel))
    log
  }

  def getAttribute(name: String)                = impl.getAttribute(name)
  def getAttributeNames                         = impl.getAttributeNames
  def release()                                 { impl.release() }
  def removeAttribute(name: String)             { impl.removeAttribute(name) }
  def setAttribute(name: String, value: AnyRef) { impl.setAttribute(name, value) }

  def getInstance(name: String)                 =
    if (name == SCOOBI_TIMES) simple
    else if (quiet)           noOps
    else                      simple

  def getInstance(klass: Class[_])              =
    if (quiet) noOps
    else       simple

  private def commonsLevel(level: Level) =
    Map(Level.INFO    -> SimpleLog.LOG_LEVEL_INFO,
        Level.ALL     -> SimpleLog.LOG_LEVEL_ALL,
        Level.CONFIG  -> SimpleLog.LOG_LEVEL_INFO,
        Level.FINE    -> SimpleLog.LOG_LEVEL_TRACE,
        Level.FINER   -> SimpleLog.LOG_LEVEL_TRACE,
        Level.FINEST  -> SimpleLog.LOG_LEVEL_TRACE,
        Level.INFO    -> SimpleLog.LOG_LEVEL_INFO,
        Level.OFF     -> SimpleLog.LOG_LEVEL_OFF,
        Level.SEVERE  -> SimpleLog.LOG_LEVEL_FATAL,
        Level.WARNING -> SimpleLog.LOG_LEVEL_WARN).getOrElse(level, SimpleLog.LOG_LEVEL_INFO)

}

object WithHadoopLogFactory {
  val SCOOBI_TIMES = "SCOOBI_TIMES"
  val QUIET        = "QUIET"
  val SHOW_TIMES   = "SHOW_TIMES"
  val LOG_LEVEL    = "LOG_LEVEL"

  def setLogFactory(name: String = classOf[WithHadoopLogFactory].getName,
                    quiet: Boolean = true,
                    showTimes: Boolean = false,
                    level: Level = Level.INFO) {
    // release any previously set LogFactory for this class loader
    LogFactory.release(Thread.currentThread.getContextClassLoader)
    setLogFactoryName(name)
    setAttributes(quiet, showTimes, level)
  }

  def setAttributes(quiet: Boolean, showTimes: Boolean, level: Level) {
    setQuiet(quiet)
    setShowTimes(showTimes)
    setLogLevel(level)
  }

  def setLogFactoryName(name: String) {
    System.setProperty("org.apache.commons.logging.LogFactory", name)
  }

  def setQuiet(quiet: Boolean = true) {
    LogFactory.getFactory.setAttribute(QUIET, quiet)
  }
  def setShowTimes(showTimes: Boolean = false) {
    LogFactory.getFactory.setAttribute(SHOW_TIMES, showTimes)
  }
  def setLogLevel(level: Level = Level.INFO) {
    LogFactory.getFactory.setAttribute(LOG_LEVEL, level)
  }
}
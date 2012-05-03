package com.nicta.scoobi.testing

import org.apache.commons.logging.LogFactory
import java.lang.Class
import org.apache.commons.logging.impl.{SimpleLog, NoOpLog, LogFactoryImpl}
import WithHadoopLogFactory._

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

  private val impl   = new LogFactoryImpl
  private val noOps  = new NoOpLog
  private val simple = new SimpleLog("TESTING")

  def getAttribute(name: String)                = impl.getAttribute(name)
  def getAttributeNames                         = impl.getAttributeNames
  def release()                                 { impl.release() }
  def removeAttribute(name: String)             { impl.removeAttribute(name) }
  def setAttribute(name: String, value: AnyRef) { impl.setAttribute(name, value) }

  def getInstance(name: String)                 =
    if (name == SCOOBI_TIMES) simple
    else if (quiet)           noOps
    else                      impl.getInstance(name)

  def getInstance(klass: Class[_])              =
    if (quiet) noOps
    else       impl.getInstance(klass)

}

object WithHadoopLogFactory {
  val SCOOBI_TIMES = "SCOOBI_TIMES"
  val QUIET        = "QUIET"
  val SHOW_TIMES   = "SHOW_TIMES"

  def setLogFactory(name: String = classOf[WithHadoopLogFactory].getName, quiet: Boolean = true, showTimes: Boolean = false) {
    // release any previously set LogFactory for this class loader
    LogFactory.release(Thread.currentThread.getContextClassLoader)
    setLogFactoryName(name)
    setAttributes(quiet, showTimes)
  }

  def setAttributes(quiet: Boolean, showTimes: Boolean) {
    setQuiet(quiet)
    setShowTimes(showTimes)
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
}
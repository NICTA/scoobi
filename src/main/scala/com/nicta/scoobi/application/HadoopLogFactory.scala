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

import java.lang.Class
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.commons.logging.impl.{SimpleLog, NoOpLog, LogFactoryImpl}
import HadoopLogFactory._
import Levels._
/**
 * Log factory used for testing.
 *
 * It doesn't display any log message by default, unless the QUIET attribute is set to true.
 *
 * It can display SCOOBI_TIMES messages if the SHOW_TIMES attributes is true
 */
class HadoopLogFactory() extends LogFactory {

  def quiet      = Option(getAttribute(QUIET)).map(_.asInstanceOf[Boolean]).getOrElse(true)
  def showTimes  = Option(getAttribute(SHOW_TIMES)).map(_.asInstanceOf[Boolean]).getOrElse(false)
  def logLevel   = Option(getAttribute(LOG_LEVEL)).map(_.asInstanceOf[Level]).getOrElse(INFO)
  def categories = Option(getAttribute(LOG_CATEGORIES)).map { c =>
    if (c == ".*") noNoise
    else           ".*"+c.toString+".*"
  }.getOrElse(noNoise)

  // see the answer to this SOF question to understand this regular expression:
  // http://stackoverflow.com/questions/406230/regular-expression-to-match-string-not-containing-a-word
  private def noWords(words: String*) = "^((?!"+words.mkString("|")+").)*$"
  private val noNoise = noWords("Client", "ProtobufRpcEngine", "RPC", "RemoteBlockReader2", "LeaseRenewer", "DFSClient", "HadoopMode")

  private val impl   = new LogFactoryImpl
  private val noOps  = new NoOpLog

  private def simple(name: String) = {
    val log = new SimpleLog(name)
    log.setLevel(commonsLevel(logLevel))
    log
  }
  private def noWarnings(name: String) = {
    val log = new SimpleLog(name)
    if (commonsLevel(logLevel) <= commonsLevel(WARN)) log.setLevel(commonsLevel(ERROR))
    else                                              log.setLevel(commonsLevel(logLevel))
    log
  }

  def getAttribute(name: String)                = impl.getAttribute(name)
  def getAttributeNames                         = impl.getAttributeNames
  def release()                                 { impl.release() }
  def removeAttribute(name: String)             { impl.removeAttribute(name) }
  def setAttribute(name: String, value: AnyRef) { impl.setAttribute(name, value) }

  def getInstance(name: String): Log      = {
    if (name == SCOOBI_TIMES)                                if (showTimes) simple(name) else noOps
    else if (quietFor(name))                                 noOps
    else if (name == "org.apache.hadoop.conf.Configuration") noWarnings(name)
    else                                                     simple(name)
  }
  def getInstance(klass: Class[_]): Log = getInstance(klass.getName)

  /** @return true if quiet or if the category 'name' doesn't match the regular expression for accepted categories */
  private def quietFor(name: String) = quiet || !name.matches(categories)

  /**
   * @return the translation between a string giving the level name and a apache commons level
   */
  private def commonsLevel(level: Level) = levelsMappings.getOrElse(level, SimpleLog.LOG_LEVEL_INFO)

}

object HadoopLogFactory {
  val SCOOBI_TIMES   = "SCOOBI_TIMES"
  val QUIET          = "QUIET"
  val SHOW_TIMES     = "SHOW_TIMES"
  val LOG_LEVEL      = "LOG_LEVEL"
  val LOG_CATEGORIES = "LOG_CATEGORIES"

  def setLogFactory(name: String = classOf[HadoopLogFactory].getName,
                    quiet: Boolean = false,
                    showTimes: Boolean = false,
                    level: String = INFO,
                    categories: String = ".*") {
    // release any previously set LogFactory for this class loader
    LogFactory.release(Thread.currentThread.getContextClassLoader)
    setLogFactoryName(name)
    setAttributes(quiet, showTimes, level, categories)
  }

  def setAttributes(quiet: Boolean, showTimes: Boolean, level: String, categories: String) {
    setQuiet(quiet)
    setShowTimes(showTimes)
    setLogLevel(level)
    setLogCategories(categories)
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
  def setLogLevel(level: String = INFO) {
    LogFactory.getFactory.setAttribute(LOG_LEVEL, level)
  }
  def setLogCategories(categories: String = ".*") {
    LogFactory.getFactory.setAttribute(LOG_CATEGORIES, categories)
  }

  lazy val levelsMappings =
    Map(ALL     -> SimpleLog.LOG_LEVEL_ALL,
        TRACE   -> SimpleLog.LOG_LEVEL_TRACE,
        INFO    -> SimpleLog.LOG_LEVEL_INFO,
        WARN    -> SimpleLog.LOG_LEVEL_WARN,
        ERROR   -> SimpleLog.LOG_LEVEL_ERROR,
        FATAL   -> SimpleLog.LOG_LEVEL_FATAL,
        OFF     -> SimpleLog.LOG_LEVEL_OFF)

  lazy val allLevels = levelsMappings.keys.map(_.toString).toSet


  lazy val ALL  : Level = level("ALL"  )
  lazy val TRACE: Level = level("TRACE")
  lazy val INFO : Level = level("INFO" )
  lazy val WARN : Level = level("WARN" )
  lazy val ERROR: Level = level("ERROR")
  lazy val FATAL: Level = level("FATAL")
  lazy val OFF  : Level = level("OFF"  )

}

trait HadoopLogFactoryInitialisation {
  // set the default HadoopLogFactory in order to intercept any log calls that will
  // be done when the Configuration object is loaded
  HadoopLogFactory.setLogFactory()
}
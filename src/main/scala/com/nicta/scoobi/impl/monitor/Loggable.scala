package com.nicta.scoobi
package impl
package monitor

import org.apache.commons.logging.Log


/**
 * syntactic sugar to log objects values
 */
trait Loggable {
  implicit def asLoggable[T](t: =>T)(implicit logger: Log): LoggableObject[T] = new LoggableObject[T](t)
  class LoggableObject[T](t: =>T)(implicit logger: Log) {
    private lazy val evaluated = t
    def debug(display: T => String)           = { logger.debug(display(evaluated)); evaluated }
    def debug(pre: String, post: String = "") = { logger.debug(pre+" "+evaluated+" "+post); evaluated }
    def info (pre: String, post: String = "") = { logger.info (pre+" "+evaluated+" "+post); evaluated }
    def warn (pre: String, post: String = "") = { logger.warn (pre+" "+evaluated+" "+post); evaluated }
    def error(pre: String, post: String = "") = { logger.error(pre+" "+evaluated+" "+post); evaluated }
  }
}
object Loggable extends Loggable

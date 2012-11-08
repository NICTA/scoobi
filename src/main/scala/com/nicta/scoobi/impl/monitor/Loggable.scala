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
    def debug(display: T => String)          : T = { logger.debug(display(evaluated)); evaluated }
    def debug(pre: String, post: String = ""): T = debug(v => pre+" "+v+" "+post)

    def info (display: T => String)          : T = { logger.info(display(evaluated)); evaluated }
    def info (pre: String, post: String = ""): T = info(v => pre+" "+v+" "+post)

    def warn (display: T => String)          : T = { logger.warn(display(evaluated)); evaluated }
    def warn (pre: String, post: String = ""): T = warn(v => pre+" "+v+" "+post)

    def error(display: T => String)          : T = { logger.error(display(evaluated)); evaluated }
    def error(pre: String, post: String = ""): T = error(v => pre+" "+v+" "+post)
  }
}
object Loggable extends Loggable

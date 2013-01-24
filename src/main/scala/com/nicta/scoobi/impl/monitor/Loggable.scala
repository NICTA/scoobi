package com.nicta.scoobi
package impl
package monitor

import org.apache.commons.logging.Log
import impl.control.Functions._
/**
 * syntactic sugar to log objects values
 */
trait Loggable {
  implicit def asBooleanLoggable(condition: Boolean)(implicit logger: Log): LoggableBooleanObject = new LoggableBooleanObject(condition)
  class LoggableBooleanObject(condition: Boolean)(implicit logger: Log) extends LoggableObject[Boolean](condition) {
    def debug(message: String): Boolean    = { if (condition) logger.debug(message); condition }
    def debugNot(message: String): Boolean = { if (!condition) logger.debug(message); condition }
  }

  /**
   * this adds debug, info, warn and error messages on any kind of object,
   * logging a message as a side-effect and returning the object
   */
  implicit def asLoggable[T](t: =>T)(implicit logger: Log): LoggableObject[T] = new LoggableObject[T](t)
  class LoggableObject[T](t: =>T)(implicit logger: Log) {
    private lazy val evaluated = t
    def debug(condition: Boolean, pre: String): T    = debug(_ => condition, v => pre+" "+v)
    def debugNot(condition: Boolean, pre: String): T = debugNot(_ => condition, v => pre+" "+v)

    def debug(condition: Boolean, display: T => String): T    = debug(_ => condition, display)
    def debugNot(condition: Boolean, display: T => String): T = debugNot(_ => condition, display)

    def debug(condition: T => Boolean, display: T => String): T    = if (condition(evaluated)) debug(display) else evaluated
    def debugNot(condition: T => Boolean, display: T => String): T = debug(!condition, display)

    def debug(pre: String, d: T => String)   : T = debug((t: T) => pre+"\n"+d(t))
    def debug(display: T => String)          : T = { logger.debug(display(evaluated)); evaluated }
    def debug(pre: String, post: String = ""): T = debug(v => pre+" "+v+" "+post)
    def debug                                : T = debug(_.toString)

    def info (display: T => String)          : T = { logger.info(display(evaluated)); evaluated }
    def info (pre: String, post: String = ""): T = info(v => pre+" "+v+" "+post)
    def info                                 : T = info(_.toString)

    def warn (display: T => String)          : T = { logger.warn(display(evaluated)); evaluated }
    def warn (pre: String, post: String = ""): T = warn(v => pre+" "+v+" "+post)
    def warn                                 : T = warn(_.toString)

    def error(display: T => String)          : T = { logger.error(display(evaluated)); evaluated }
    def error(pre: String, post: String = ""): T = error(v => pre+" "+v+" "+post)
    def error                                : T = error(_.toString)

  }
}
object Loggable extends Loggable

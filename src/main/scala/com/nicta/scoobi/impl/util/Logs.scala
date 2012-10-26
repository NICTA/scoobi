package com.nicta.scoobi
package impl
package util

import org.apache.commons.logging.Log

object Logs {
  implicit def loggable[T](t: T)(implicit logger: Log): Loggable[T] = new Loggable(t)
  class Loggable[T](t: T)(implicit logger: Log) {
    def debug(msg: String)      = { logger.debug(msg); t }
    def debug(msg: T => String) = { logger.debug(msg(t)); t }
    def info(msg: String)       = { logger.info(msg); t }
    def info(msg: T => String)  = { logger.info(msg(t)); t }
    def warn(msg: String)       = { logger.warn(msg); t }
    def warn(msg: T => String)  = { logger.warn(msg(t)); t }
    def error(msg: String)      = { logger.error(msg); t }
    def error(msg: T => String) = { logger.error(msg(t)); t }

  }
}




package com.nicta.scoobi
package impl
package control

private[scoobi]
trait SystemProperties {
  def getEnv(name: String): Option[String] = Option(System.getenv(name))
}

private[scoobi]
object SystemProperties extends SystemProperties

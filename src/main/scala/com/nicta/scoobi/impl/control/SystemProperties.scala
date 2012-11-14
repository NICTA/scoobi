package com.nicta.scoobi
package impl
package control

/**
 * This trait is introduced to facilitate testing when accessing system properties
 */
private[scoobi]
trait SystemProperties {
  def getEnv(name: String): Option[String] = Option(System.getenv(name))
  def get(name: String): Option[String]    = sys.props.get(name)
}

private[scoobi]
object SystemProperties extends SystemProperties

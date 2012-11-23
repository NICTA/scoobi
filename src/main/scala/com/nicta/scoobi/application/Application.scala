package com.nicta.scoobi
package application

trait Application {
  type ScoobiApp = com.nicta.scoobi.application.ScoobiApp
  type ScoobiConfiguration = com.nicta.scoobi.core.ScoobiConfiguration
  val ScoobiConfiguration = com.nicta.scoobi.application.ScoobiConfiguration
}
object Application extends Application



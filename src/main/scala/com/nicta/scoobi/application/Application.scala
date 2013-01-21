package com.nicta.scoobi
package application

trait Application {
  type ScoobiApp = com.nicta.scoobi.application.ScoobiApp
  type ScoobiConfiguration = com.nicta.scoobi.core.ScoobiConfiguration
}
object Application extends Application



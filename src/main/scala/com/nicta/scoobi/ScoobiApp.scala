package com.nicta.scoobi

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.conf.Configuration

trait ScoobiApp {

  protected var args: Array[String] = _

  protected implicit val configuration: ScoobiConfiguration = ScoobiConfiguration()

  def run()

  def main(arguments: Array[String]) {
    configuration.withHadoopArgs(arguments) { remainingArgs =>
      args = remainingArgs
      run()
    }
  }
}

package com.nicta.scoobi

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.conf.Configuration

trait ScoobiApp extends DelayedInit {

  private var initCode: () => Unit = _

  protected var args: Array[String] = _

  protected implicit val configuration: ScoobiConfiguration = ScoobiConfiguration()

  override def delayedInit(body: => Unit) {
     initCode = (() => body)
  }

  def main(arguments: Array[String]) {
    configuration.withHadoopArgs(arguments) { remainingArgs =>
      args = remainingArgs
      initCode()
    }
  }
}

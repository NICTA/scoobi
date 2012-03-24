package com.nicta.scoobi

import com.nicta.scoobi.Scoobi._

trait ScoobiApp extends DelayedInit {

  private var initCode: () => Unit = _

  protected var args: Array[String] = _

  override def delayedInit(body: => Unit) {
     initCode = (() => body)
  }

  def main(arguments: Array[String]) = withHadoopArgs(arguments) { remainingArgs =>
    args = remainingArgs
    initCode()
  }
}

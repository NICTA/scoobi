package com.nicta.scoobi
package application

import org.apache.hadoop.fs._
import scala.tools.nsc.Settings
import tools.nsc.interpreter.{AbstractFileClassLoader, ILoop}
import core.ScoobiConfiguration
import scala.collection.JavaConversions._

/** A REPL for Scoobi.
  *
  * Run the 'scoobi' script, which will bring you into the Scala REPL
  *
  * You're now good to go!! */
object ScoobiRepl extends ScoobiApp with ReplFunctions {

  class ScoobiILoop extends ILoop {
    override def prompt = "scoobi> "


    addThunk {
      println("\n === Please wait while Scoobi is initialising... ===")

      intp.beQuietDuring {
      intp.addImports("java.lang.Math._")
      intp.addImports("com.nicta.scoobi.Scoobi._")
      intp.addImports("com.nicta.scoobi.application.ScoobiRepl._")
      intp.addImports("scala.collection.JavaConversions._")
      replConfiguration.addClassLoader(intp.classLoader)
      woof()
    }}
  }

  def run() {
    val settings = new Settings
    settings.usejavacp.value = true
    new ScoobiILoop().process(settings)
  }

  def woof() {
    println(splash)
    println("\n === Ready, press Enter to start ===")
  }

  lazy val splash: String =
    """|
       |                                                     |\
       |                                             /    /\/o\_
       |                              __    _       (.-.__.(   __o
       |       ______________  ____  / /_  (_)   /\_(      .----'
       |      / ___/ ___/ __ \/ __ \/ __ \/ /     .' \____/
       |     (__  ) /__/ /_/ / /_/ / /_/ / /     /   /  / \
       |    /____/\___/\____/\____/_.___/_/ ____:____\__\__\____________""".stripMargin

}

trait ReplFunctions { this: { def configuration: ScoobiConfiguration } =>
  /** List a path . */
  def ls(path: String) {
    configuration.fileSystem.listStatus(new Path(path)) foreach { fstat =>
      Console.printf("%s%s  %-15s  %-12s  %s\n",
        if (fstat.isFile) "-" else "d",
        fstat.getPermission,
        fstat.getOwner,
        fstat.getBlockSize,
        fstat.getPath.toUri.getPath)
    }
  }

  /** Get the contents of a text file. */
  def cat(path: String): Iterable[String] = {
    import scala.io._
    Source.fromInputStream(configuration.fileSystem.open(new Path(path))).getLines().toIterable
  }

}


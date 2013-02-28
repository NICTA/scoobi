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
object ScoobiRepl extends ScoobiInterpreter

/** This trait can be extended if you want to provide alternative imports or behaviors
  *
  */
trait ScoobiInterpreter extends ScoobiApp with ReplFunctions {

  def imports: Seq[String] = Seq(
    "com.nicta.scoobi.Scoobi._",
    "com.nicta.scoobi.application.ScoobiRepl._",
    "scala.collection.JavaConversions._")

  /**
   * definition of the interpreter loop
   */
  class ScoobiILoop extends ILoop {
    override def prompt = if (intp.isInitializeComplete) "scoobi> " else ""
    override def printWelcome() {
      println("\n === Please wait while Scoobi is initialising... ===")
    }

    addThunk {
      intp.beQuietDuring {
        imports.foreach(i => intp.addImports(i))
        configuration.addClassLoader(intp.classLoader)
        configuration.jobNameIs("REPL")
        woof()
      }
    }
  }

  /** start the program but don't upload jars by default */
  override def main(args: Array[String]) {
    parseHadoopArguments(args)
    onHadoop {
      try { run }
      finally { if (!keepFiles) { configuration.deleteWorkingDirectory } }
    }
  }

  def run() {
    val settings = new Settings
    settings.usejavacp.value = true
    new ScoobiILoop().process(settings)
  }

  /**
   * announce that the Scoobi REPL is ready
   */
  def woof() {
    println(splash)
    println("\n\n === Ready, press Enter to start ===")
  }

  def help =
    """|
       |Available commands:
       |
       | help               the list of commands
       | inmemory           set the execution mode to in-memory
       | local              set the execution mode to local
       | cluster (default)  set the execution mode to cluster (using the configuration from $HADOOP_HOME/conf)
       |
       |""".stripMargin

  /** set the configuration so that the next job is run in memory */
  def inmemory { configureForInMemory }
  /** set the configuration so that the next job is run locally */
  def local    { configureForLocal }
  /** set the configuration so that the next job is run on the cluster - this is the default */
  def cluster  {
    setConfiguration(configuration.configuration)
    configureForCluster
  }

  def scoobiArgs_=(arguments: String) {
    scoobiArguments = arguments.split("\\.").map(_.trim)
    setLogFactory()
  }

  override def useHadoopConfDir = true

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


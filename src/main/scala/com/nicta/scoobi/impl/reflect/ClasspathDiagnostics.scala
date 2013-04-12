package com.nicta.scoobi.impl.reflect

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.avro.Schema
import Classes._
import org.apache.hadoop.io.Writable
import com.nicta.scoobi.core.ScoobiConfiguration

/**
 * This object prints out the originating jar for the most important libraries used in Scoobi
 * so that it's easier to debug classpath errors.
 *
 * To use this object you need to have an implicit logger object in scope and call the logDebug method
 *
 */
object ClasspathDiagnostics {

  def logDebug(implicit logger: Log) {
    Seq(
      ("Java",   classOf[java.lang.String]),
      ("Hadoop", classOf[Writable]),
      ("Avro",   classOf[Schema]),
      ("Scoobi", classOf[ScoobiConfiguration])
    ).foreach { case (lib, c) => logDebugClass(lib, c.getName) }
  }

  private def logDebugClass(libName: String, className: String)(implicit logger: Log) {
    try logger.debug(s"the URL of $libName (evidenced with the $className class) is "+getClass.getClassLoader.getResource(filePath(className)+".class"))
    catch { case e: Exception => e.printStackTrace  }
  }

}

/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi.impl.reflect

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.avro.Schema
import Classes._
import org.apache.hadoop.io.Writable
import com.nicta.scoobi.core.ScoobiConfiguration
import org.kiama.rewriting.Rewriter

/**
 * This object prints out the originating jar for the most important libraries used in Scoobi
 * so that it's easier to debug classpath errors.
 *
 * To use this object you need to have an implicit logger object in scope and call the logDebug method
 *
 */
object ClasspathDiagnostics {

  def logInfo(implicit logger: Log) { logFiles(logger.info _) }
  def logDebug(implicit logger: Log) { logFiles(logger.debug _) }

  private def logFiles(logFunction: String => Unit) {
    Seq(
      ("Java",   classOf[java.lang.String]),
      ("Scala",   classOf[scala.Range]),
      ("Hadoop", classOf[Writable]),
      ("Avro",   classOf[Schema]),
      ("Kiama",   classOf[Rewriter]),
      ("Scoobi", classOf[ScoobiConfiguration])
    ).foreach { case (lib, c) => logDebugClass(lib, c.getName)(logFunction) }
  }

  private def logDebugClass(libName: String, className: String)(logFunction: String => Unit) {
    try logFunction(s"the URL of $libName (evidenced with the $className class) is "+getClass.getClassLoader.getResource(filePath(className)))
    catch { case e: Exception => e.printStackTrace  }
  }

}

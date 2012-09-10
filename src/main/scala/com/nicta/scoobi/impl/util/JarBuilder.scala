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
package com.nicta.scoobi
package impl
package util

import scala.collection.JavaConversions._
import scala.collection.mutable.{Set => MSet}
import Option.{apply => ?}

import java.util.jar.JarInputStream
import java.util.jar.JarOutputStream
import java.util.jar.JarEntry
import java.io._
import java.net.{URLClassLoader, URLDecoder}


import impl.rtt.RuntimeClass
import application.ScoobiConfiguration
import JarBuilder._
import org.apache.commons.logging.LogFactory


/** Class to manage the creation of a new JAR file. */
class JarBuilder(implicit configuration: ScoobiConfiguration) {
  private lazy val logger = LogFactory.getLog("scoobi.JarBuilder")

  private val jos = new JarOutputStream(new FileOutputStream(configuration.temporaryJarFile.getAbsolutePath))
  private val entries: MSet[String] = MSet.empty

  /** Merge in the contents of an entire JAR. */
  def addJar(jarFile: String) { addJarEntries(jarFile, e => true) }

  /** Add the entire contents of a JAR that contains a particular class. */
  def addContainingJar(clazz: Class[_]) {
    findContainingJar(clazz).foreach(addJar(_))
  }

  /** Add a class that has been loaded and is contained in some exising JAR. */
  def addClass(clazz: Class[_]) {
    findContainingJar(clazz).foreach(addJarEntries(_, (mkClassFile(clazz) == _.getName)))
  }

  /** Add the class files found in a given directory */
  def addClassDirectory(path: String) {
    def addSubDirectory(p: String, baseDirectory: String) {
      Option(new File(p).listFiles: Seq[File]).getOrElse(Nil).foreach { f =>
        if (f.isDirectory) addSubDirectory(f.getPath, baseDirectory)
        else {
          val stream = new FileInputStream(f)
          try { addEntryFromStream(f.getPath.replace(baseDirectory, ""), stream) } finally { stream.close() }
        }
      }

    }
    addSubDirectory(path, path)
  }

  /** Add a single class to the JAR where its bytecode is given directly. */
  def addClassFromBytecode(className: String, bytecode: Array[Byte]) {
    addEntryFromStream(className + ".class", new ByteArrayInputStream(bytecode))
  }

  /** Add a class to the JAR that was generated at runtime. */
  def addRuntimeClass(runtimeClass: RuntimeClass) {
    addClassFromBytecode(runtimeClass.name, runtimeClass.bytecode)
  }

  /** Write-out the JAR file. Once this method is called, the JAR cannot be
    * modified further. */
  def close(implicit configuration: ScoobiConfiguration) {
    jos.close()
    if (configuration.getClassLoader.isInstanceOf[URLClassLoader]) {
      logger.debug("adding the temporary jar entries to the current classloader")

      val loader = configuration.getClassLoader.asInstanceOf[URLClassLoader]
       invoke(loader, "addURL", Array(new File(configuration.temporaryJarFile.getName).toURI.toURL))
       // load the classes right away so that they're always available
       // otherwise the job jar will be removed when the MapReduce job
       // has finished running and the classes won't be available for further use
       // like acceptance tests where we need to read the results from a SequenceFile
      entries.foreach { e =>
        try { loader.loadClass(e) }
        // this might fail for some entries like scala/util/continuations/ControlContext*
        catch { case ex: Throwable => () }

      }
    } else logger.debug("cannot add the temporary jar to the current classloader because this is not a URLClassLoader")
  }

  private def invoke[T](t: T, method: String, params: Array[AnyRef]) {
    try {
      val m = t.getClass.getDeclaredMethod(method, params.map(_.getClass):_*)
      m.setAccessible(true)
      import scala.collection.JavaConversions._
      m.invoke(t, asJavaCollection(params.toList).toArray:_*)
    }
    // the invocation will fail when run from sbt in the 'local' mode
    // so don't log anything
    catch { case e => () }
  }



  /** Add an entry to the JAR file from an input stream. If the entry already exists,
    * do not add it. */
  private def addEntryFromStream(entryName: String, is: InputStream) {
    if (!entries.contains(entryName)) {
      entries += entryName
      jos.putNextEntry(new JarEntry(entryName))
      val buffer: Array[Byte] = new Array(1024)
      var readCnt = 0
      while ({readCnt = is.read(buffer); readCnt > 0}) {
        jos.write(buffer, 0, readCnt)
      }
    }
  }

  /** Add entries for an existing JAR file based on some predicate. */
  private def addJarEntries(jarFile: String, p: JarEntry => Boolean) {
    val jis = new JarInputStream(new FileInputStream(jarFile))
    Stream.continually(jis.getNextJarEntry).takeWhile(_ != null) foreach { entry =>
      if (p(entry)) {
        val name: String = entry.getName
        addEntryFromStream(name, jis)
      }
    }
    jis.close()
  }
}

object JarBuilder {

  /** Find the location of JAR that contains a particular class. */
  def findContainingJar(clazz: Class[_]): Option[String] = {

    val classFile = mkClassFile(clazz)
    val loader = ?(clazz.getClassLoader) match {
      case Some(l) => l
      case None    => ClassLoader.getSystemClassLoader
    }

    val foundPaths =
      (for {
        url <- loader.getResources(classFile)
        if ("jar" == url.getProtocol)
        path = url.getPath.replaceAll("file:", "")
                          .replaceAll("\\+", "%2B")
                          .replaceAll("!.*$", "")
      } yield URLDecoder.decode(path, "UTF-8")).toList

    if (foundPaths.isEmpty)
      None
    else
      Some(foundPaths.head)
  }

  /** Return the class file path string as specified in a JAR for a give class. */
  private def mkClassFile(clazz: Class[_]): String = {
    clazz.getName.replaceAll("\\.", "/") + ".class"
  }
}

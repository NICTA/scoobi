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
package rtt

import scala.collection.mutable.{Set => MSet}

import java.util.jar.{JarInputStream, JarOutputStream, JarEntry}
import java.io._
import java.net.URLClassLoader
import org.apache.commons.logging.LogFactory


import core.ScoobiConfiguration
import reflect.Classes._
import ScoobiConfigurationImpl._

/** Class to manage the creation of a new JAR file. */
class JarBuilder(implicit configuration: ScoobiConfiguration) {
  private lazy val logger = LogFactory.getLog("scoobi.JarBuilder")

  private val jos = new JarOutputStream(new FileOutputStream(configuration.temporaryJarFile.getAbsolutePath))
  private val entries: MSet[String] = MSet.empty

  /** Merge in the contents of an entire JAR. */
  def addJar(jarFile: String) {
    addJarEntries(jarFile, e => true)
  }

  /** Add the entire contents of a JAR that contains a particular class. */
  def addContainingJar(clazz: Class[_]) {
    val jar = findContainingJar(clazz)
    logger.debug("adding jar entries for class "+clazz.getName+" from jar "+jar.getOrElse("<no jar found>"))
    jar.foreach(addJar(_))
  }

  /** Add a class that has been loaded and is contained in some existing JAR. */
  def addClass(clazz: Class[_]) {
    findContainingJar(clazz).foreach(addJarEntries(_, (filePath(clazz) == _.getName)))
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
    addEntryFromStream(className.replace(".", "/") + ".class", new ByteArrayInputStream(bytecode))
  }

  /** Add a class to the JAR that was generated at runtime. */
  def addRuntimeClass(runtimeClass: RuntimeClass) {
    addClassFromBytecode(runtimeClass.name, runtimeClass.bytecode)
  }

  /** Write-out the JAR file. Once this method is called, the JAR cannot be
    * modified further. */
  def close(implicit configuration: ScoobiConfiguration) {
    jos.close()
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


/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.impl.util

import scala.collection.JavaConversions._
import scala.collection.Traversable._
import scala.collection.mutable.{Set => MSet}
import Option.{apply => ?}

import java.util.jar.JarInputStream
import java.util.jar.JarOutputStream
import java.util.jar.JarEntry

import java.io.InputStream
import java.io.FileInputStream
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream

import java.io.FileOutputStream
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream

import java.net.URLDecoder

import com.nicta.scoobi.impl.rtt.RuntimeClass


/** Class to manage the creation of a new JAR file. */
class JarBuilder(val name: String) {

  import JarBuilder._

  private val jos = new JarOutputStream(new FileOutputStream(name))
  private val entries: MSet[String] = MSet.empty

  /** Merge in the contents of an entire JAR. */
  def addJar(jarFile: String): Unit = addJarEntries(jarFile, e => true)

  /** Add the entire contents of a JAR that contains a particular class. */
  def addContainingJar(clazz: Class[_]): Unit = addJar(findContainingJar(clazz))

  /** Add a class that has been loaded and is contained in some exising JAR. */
  def addClass(clazz: Class[_]): Unit =
    addJarEntries(findContainingJar(clazz), (mkClassFile(clazz) == _.getName))

  /** Add a single class to the JAR where its bytecode is given directly. */
  def addClassFromBytecode(className: String, bytecode: Array[Byte]): Unit = {
    addEntryFromStream(className + ".class", new ByteArrayInputStream(bytecode))
  }

  /** Add a class to the JAR that was generated at runtime. */
  def addRuntimeClass(runtimeClass: RuntimeClass): Unit = {
    addClassFromBytecode(runtimeClass.name, runtimeClass.bytecode)
  }

  /** Write-out the JAR file. Once this method is called, the JAR cannot be
    * modified further. */
  def close() = {
    jos.close()
  }


  /** Add an entry to the JAR file from an input stream. If the entry already exists,
    * do not add it. */
  private def addEntryFromStream(entryName: String, is: InputStream): Unit = {
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
  private def addJarEntries(jarFile: String, p: JarEntry => Boolean): Unit = {
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
  def findContainingJar(clazz: Class[_]): String = {

    val classFile = mkClassFile(clazz)
    val loader = ?(clazz.getClassLoader) match {
      case Some(l) => l
      case None    => ClassLoader.getSystemClassLoader
    }

    val foundPaths =
      for {
        url <- loader.getResources(classFile)
        if ("jar" == url.getProtocol || "file" == url.getProtocol)
        path = url.getPath.replaceAll("file:", "")
                          .replaceAll("\\+", "%2B")
                          .replaceAll("!.*$", "")
      } yield URLDecoder.decode(path, "UTF-8")

    foundPaths.toList.head
  }

  /** Return the class file path string as specified in a JAR for a give class. */
  private def mkClassFile(clazz: Class[_]): String = {
    clazz.getName().replaceAll("\\.", "/") + ".class"
  }
}

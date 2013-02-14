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
package reflect

import java.util.jar.{JarEntry, JarInputStream}
import java.io.FileInputStream
import java.net.{URL, URLDecoder}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import core.DList
import monitor.Loggable._
import org.apache.commons.logging.LogFactory

import control.Exceptions._

/**
 * Utility methods for accessing classes and methods
 */
private[scoobi]
trait Classes {

  private implicit lazy val logger = LogFactory.getLog("scoobi.Classes")

  /** @return the class with a main method calling this code */
  def mainClass = loadClass(mainClassName)

  /** @return the class with a main method calling this code */
  def mainClassName =
    mainStackElement.getClassName.debug("the main class for this application is")

  /** @return the first stack element being a main method (from the bottom of the stack) */
  def mainStackElement: StackTraceElement =
    lastStackElementWithMethodName("main")

  /** @return the last stackElement having this method name, or the first stack element of the current stack */
  def lastStackElementWithMethodName(name: String): StackTraceElement =
    (new Exception).getStackTrace.toSeq.filterNot(e => Seq("scala.tools", "org.apache.hadoop").exists(e.getClassName.contains)).
                                        filter(_.getMethodName == name).lastOption.
      getOrElse((new Exception).getStackTrace.apply(0))

  /** @return the jar entries of the jar file containing the main class */
  def mainJarEntries: Seq[JarEntry] =
    findContainingJar(mainClassName).map(jarEntries).getOrElse(Seq())

  /** @return true if at least one of the entries in the main jar is a Scoobi class, but not an example */
  def mainJarContainsDependencies = {
    val scoobiEntry = mainJarEntries.find(_.getName.contains(filePath(classOf[DList[Int]]))).
                      debug("Scoobi entry found in the main jar")
    scoobiEntry.isDefined
  }

  /** @return the entries for a given jar path */
  def jarEntries(jarPath: String): Seq[JarEntry] = {
    val entries = new ListBuffer[JarEntry]
    val jis = new JarInputStream(new FileInputStream(jarPath))
    Stream.continually(jis.getNextJarEntry).takeWhile(_ != null) foreach { entry =>
      entries += entry
    }
    jis.close()
    entries
  }

  /** Find the location of JAR that contains a particular class. */
  def findContainingJar(clazz: Class[_]): Option[String] =
    findContainingJar(loader(clazz), clazz.getName)

  /** Find the location of JAR that contains a particular class name. */
  private def findContainingJar(className: String): Option[String] =
    findContainingJar(loader(getClass), className)

  /** Find the location of JAR for a given class loader and a particular class name */
  private def findContainingJar(loader: ClassLoader, className: String): Option[String] = {
    val resources: Seq[String] = loader.getResources(filePath(className)).toIndexedSeq.view.filter(_.getProtocol == "jar").map(filePath)
    resources.headOption match {
      case jar @ Some(_) => jar.debug("the jar containing the class "+className+" is")
      case _             => None.debug("could not find the jar for class "+className+" in the following loader "+loader+" having those resources\n"+resources.mkString("\n"))
    }
  }

  /** Return the class file path string as specified in a JAR for a given class. */
  def filePath(clazz: Class[_]): String = filePath(clazz.getName)

  /** Return the class file path string as specified in a JAR for a given class name. */
  def filePath(className: String): String =
    className.replaceAll("\\.", "/") + ".class"

  /** @return the file path corresponding to a full URL */
  private def filePath(url: URL): String =
    URLDecoder.decode(url.getPath.replaceAll("file:", "").replaceAll("\\+", "%2B").replaceAll("!.*$", ""), "UTF-8")

  /** @return the classLoader for a given class */
  private def loader(clazz: Class[_]) =
    Option(clazz.getClassLoader).getOrElse(ClassLoader.getSystemClassLoader)

  private def loadClass[T](name: String) =
    loadClassOption(name).orElse(None.debug("could not load class "+name)).getOrElse(getClass.asInstanceOf[Class[T]])

  private def loadClassOption[T](name: String): Option[Class[T]] =
    Seq(getClass.getClassLoader, ClassLoader.getSystemClassLoader).view.map { loader =>
      trye(loader.loadClass(name).asInstanceOf[Class[T]]) match {
        case Left(e) => {
          e.debug(ex => "could not load class "+name+" with classloader "+loader+" because "+e.getMessage)
          Option(e.getCause).foreach(_.debug(c => "caused by "+c.getMessage))
          e.getStackTrace.foreach(st => st.debug(""))
          None
        }
        case Right(a) => Some(a)
      }
    }.flatten.headOption
}

object Classes extends Classes

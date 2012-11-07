package com.nicta.scoobi
package impl
package reflect

import control.Exceptions._
import java.util.jar.{JarEntry, JarInputStream}
import java.io.{File, FileInputStream}
import scala.Option
import java.net.{URL, URLDecoder}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * Utility methods for accessing classes and methods
 */
//private[scoobi]
trait Classes {

  /** @return the class with a main method calling this code */
  def mainClass =
    loadClass(mainStackElement.getClassName)

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
    findContainingJar(mainClass).map(jarEntries).getOrElse(Seq())

  /** @return true if at least one of the entries in the main jar is a Scoobi class, but not an example */
  def mainJarContainsDependencies =
    mainJarEntries.exists(e => (e.getName.startsWith("com/nicta/scoobi") && !e.getName.contains("examples")) ||
                                e.getName.matches(".*scoobi_.*.jar"))

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
    loader(clazz).getResources(filePath(clazz)).toIndexedSeq.view.filter(_.getProtocol == "jar").map(filePath).headOption

  /** Return the class file path string as specified in a JAR for a give class. */
  def filePath(clazz: Class[_]): String =
    clazz.getName.replaceAll("\\.", "/") + ".class"

  /** @return the file path corresponding to a full URL */
  private def filePath(url: URL): String =
    filePath(url.getPath)

  /** @return the file path corresponding to a full URL */
  private def filePath(urlPath: String): String =
    URLDecoder.decode(urlPath.replaceAll("file:", "").replaceAll("\\+", "%2B").replaceAll("!.*$", ""), "UTF-8")

  /** @return the classLoader for a given class */
  private def loader(clazz: Class[_]) =
    Option(clazz.getClassLoader).getOrElse(ClassLoader.getSystemClassLoader)

  private def loadClass[T](name: String) =
    loadClassOption(name).getOrElse(getClass.asInstanceOf[Class[T]])

  private def loadClassOption[T](name: String): Option[Class[T]] =
    tryo(getClass.getClassLoader.loadClass(name).asInstanceOf[Class[T]])
}

//private[scoobi]
object Classes extends Classes

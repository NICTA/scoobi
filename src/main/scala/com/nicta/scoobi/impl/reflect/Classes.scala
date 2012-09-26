package com.nicta.scoobi
package impl
package reflect

import control.Exceptions._

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

  private def loadClass[T](name: String) =
    loadClassOption(name).getOrElse(getClass.asInstanceOf[Class[T]])

  private def loadClassOption[T](name: String): Option[Class[T]] =
    tryo(getClass.getClassLoader.loadClass(name).asInstanceOf[Class[T]])
}

//private[scoobi]
object Classes extends Classes

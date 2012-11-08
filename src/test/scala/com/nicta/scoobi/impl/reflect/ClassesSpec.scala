package com.nicta.scoobi
package impl
package reflect

import testing.mutable.UnitSpecification
import java.util.jar.JarEntry
import core.DList
import java.util

class ClassesSpec extends UnitSpecification {
  "the main jar contains all the dependent jars classes if it contains the DList scoobi class" >> {
    val classesWithDependencies = new Classes {
      override def mainJarEntries = Seq(new JarEntry(classOf[DList[String]].getName.split("\\.").mkString("/")))
    }
    val classesWithoutDependencies = new Classes {
      override def mainJarEntries = Seq(new JarEntry(classOf[util.ArrayList[Int]].getName.split("\\.").mkString("/")))
    }

    classesWithDependencies.mainJarContainsDependencies must beTrue
    classesWithoutDependencies.mainJarContainsDependencies must beFalse
  }
}

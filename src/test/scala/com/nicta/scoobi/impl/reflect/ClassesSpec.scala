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
      override def mainJarEntries = Seq(new JarEntry("com/nicta/scoobi/core/DList.class"))
    }
    val classesWithoutDependencies = new Classes {
      override def mainJarEntries = Seq(new JarEntry("java/util/ArrayList.class"))
    }

    classesWithDependencies.mainJarContainsDependencies must beTrue
    classesWithoutDependencies.mainJarContainsDependencies must beFalse
  }
}

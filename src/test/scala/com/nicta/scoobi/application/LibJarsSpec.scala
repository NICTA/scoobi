package com.nicta.scoobi
package application

import testing.mutable.UnitSpecification
import LibJars._
import java.io.File
import org.specs2.specification.Scope
import io.FileSystems
import impl.control.SystemProperties

class LibJarsSpec extends UnitSpecification {

  "The libjars trait is used to determine if the client jar is a 'fat' jar or not and if so, if all its dependencies"+
  "should be uploaded to the cluster".newp

  "JarOnPath".txt
  "The list of jars for a path representing a single jar is itself" >> {
   jarsOnPath("/tmp/lib/scoobi.jar") === Seq(new File("/tmp/lib/scoobi.jar").toURI.toURL)
  }
  "The list of jars for a path representing a directory with a /* is all the jars in that directory" >> new libjars{
    jarsOnPath("/tmp/lib/*") === Seq("/tmp/lib/a.jar", "/tmp/lib/b.jar").map(f => new File(f).toURI.toURL)
  }

  "Jars can be found from the HADOOP_CLASSPATH variable" >> new libjars {
    hadoopClasspathJars must have size (5)
  }


  /**
   * This mimicks the situation where there are 5 jars to be found, with one single entry and 2 directories
   */
  trait libjars extends LibJars with Scope {
    override lazy val sysProps = new SystemProperties {
      override def getEnv(name: String) = Map("HADOOP_CLASSPATH" -> Seq("/tmp/lib0/*", "/tmp/lib/*", "home/lib/one.jar").mkString(File.pathSeparator)).get(name)
    }
    override lazy val fss = new FileSystems {
      override def listFilePaths(path: String) =
        if (path == "/tmp/lib") Seq("/tmp/lib/a.jar", "/tmp/lib/b.jar")
        else Seq("/tmp/lib0/a0.jar", "/tmp/lib0/b0.jar")
    }
  }
}

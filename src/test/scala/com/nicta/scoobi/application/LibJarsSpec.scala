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

  "Jar upload".newp

  "if deleteLibJars = true, the libjars directory must be deleted before the upload is done" >> new libjars {
    uploadLibJarsFiles(deleteLibJarsFirst = true)(sc)
    deleted must beSome("libjars/")
  }

  "the jars are uploaded in a `libjars` directory" >> {
    "by default that directory is `libjars`" >> new libjars {
      libjarsDirectory === "libjars/"
    }
    "but this can be overridden by the scoobi.libjarsdir property" >> new libjars {
      override lazy val sysProps = new SystemProperties {
        override def get(name: String) = Some("/tmp/jars")
      }
      libjarsDirectory === "/tmp/jars/"
    }
  }


  /**
   * This mimicks the situation where there are 5 jars to be found, with one single entry and 2 directories
   */
  trait libjars extends LibJars with Scope {
    var deleted: Option[String] = None

    implicit val sc = ScoobiConfiguration()

    override lazy val sysProps = new SystemProperties {
      override def getEnv(name: String) = Map("HADOOP_CLASSPATH" -> Seq("/tmp/lib0/*", "/tmp/lib/*", "home/lib/one.jar").mkString(File.pathSeparator)).get(name)
    }
    override lazy val fss = new FileSystems {
      override def listFilePaths(path: String) =
        if (path == "/tmp/lib") Seq("/tmp/lib/a.jar", "/tmp/lib/b.jar")
        else Seq("/tmp/lib0/a0.jar", "/tmp/lib0/b0.jar")

      override def deleteFiles(dest: String)(implicit configuration: ScoobiConfiguration) {
        deleted = Some(dest)
      }

      // don't create any directory for the tests
      override def mkdir(dir: String)(implicit configuration: ScoobiConfiguration) {}
      // don't upload anything really
      override def uploadNewJars(sourceFiles: Seq[File], dest: String)(implicit configuration: ScoobiConfiguration): Seq[File] = sourceFiles
    }
  }
}

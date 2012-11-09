package com.nicta.scoobi
package testing

import application.ScoobiConfiguration
import org.specs2.main.Arguments
import org.specs2.reporter.ConsoleReporter
import org.specs2.specification.{ExecutedSpecification, ExecutingSpecification, SpecificationStructure}

class HadoopSpecificationStructureSpec extends mutable.UnitSpecification {

  "if the examples are running the cluster context then the jars must be uploaded to the cluster" >> {
    var jarsUploaded = false
    val s = new HadoopSpecification {
      override lazy val scoobiArgs = Seq("cluster")
      def is = "test" ! { sc: ScoobiConfiguration =>
        ok
      }
      override def uploadLibJarsFiles(implicit sc: ScoobiConfiguration) = { jarsUploaded = true }
    }
    silentRunner.run(s)

    "the jars have been uploaded" ==> { jarsUploaded === true }
  }
}

object silentRunner {
  lazy val reporter = new ConsoleReporter {
    override def export(implicit args: Arguments) = (s: ExecutingSpecification) => s.executed
  }
  def run(spec: SpecificationStructure) = reporter.report(spec)(Arguments())
}


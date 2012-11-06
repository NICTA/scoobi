package com.nicta.scoobi
package testing

import application.ScoobiConfiguration

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
    specs2.run(s)

    "the jars have been uploaded" ==> { jarsUploaded === true }
  }
}

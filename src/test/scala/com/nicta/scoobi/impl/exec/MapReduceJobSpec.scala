package com.nicta.scoobi
package impl
package exec

import org.specs2.mock.Mockito
import util.JarBuilder
import application.ScoobiConfiguration
import org.specs2.specification.Outside
import testing.mutable.UnitSpecification

class MapReduceJobSpec extends UnitSpecification with Mockito { isolated

  implicit protected def configuration = new Outside[ScoobiConfiguration] { def outside = ScoobiConfiguration() }

  "A MapReduceJob must be configured" >> {
    "all the necessary classes must be added to a jar sent to the cluster" >> {
      val jar = mock[JarBuilder]

      "if the dependent jars have not been uploaded then the Scoobi jar must be added to the JarBuilder" >> { implicit sc: ScoobiConfiguration =>
        sc.setUploadedLibJars(uploaded = false)
        new MapReduceJob(1).configureJar(jar)
        there was one(jar).addContainingJar(any[Class[_]])
      }
      "if the dependent jars have been uploaded then the Scoobi jar must not be added to the JarBuilder" >> { implicit sc: ScoobiConfiguration =>
        sc.setUploadedLibJars(uploaded = true)
        new MapReduceJob(1).configureJar(jar)
        there was no(jar).addContainingJar(any[Class[_]])
      }
    }
  }
}

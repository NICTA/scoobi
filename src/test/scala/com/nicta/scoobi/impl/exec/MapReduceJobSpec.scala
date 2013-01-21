package com.nicta.scoobi
package impl
package exec

import org.apache.hadoop.fs.Path
import org.specs2.mock.Mockito
import org.specs2.specification.Outside
import rtt.JarBuilder

import testing.mutable.UnitSpecification
import impl.ScoobiConfiguration
import core.ScoobiConfiguration
import org.specs2.execute.Pending

class MapReduceJobSpec extends UnitSpecification with Mockito { isolated

  implicit protected def configuration = new Outside[ScoobiConfiguration] { def outside = ScoobiConfiguration() }

  "A MapReduceJob must be configured" >> {
    "all the necessary classes must be added to a jar sent to the cluster" >> {
      val jar = mock[JarBuilder]

      "if the dependent jars have not been uploaded then the Scoobi jar must be added to the JarBuilder" >> { implicit sc: ScoobiConfiguration =>
        sc.setUploadedLibJars(uploaded = false)
        MapReduceJob.configureJar(jar)
        there was two(jar).addContainingJar(any[Class[_]])
      }
      "if the dependent jars have been uploaded then the Scoobi jar must not be added to the JarBuilder" >> { implicit sc: ScoobiConfiguration =>
        sc.setUploadedLibJars(uploaded = true)
        MapReduceJob.configureJar(jar)
        there was no(jar).addContainingJar(any[Class[_]])
      }
    }
  }
  "At the end of the job execution the outputs must be collected" >> {
    // mock the file system interactions
//    val (sink, reducer, fss, files) = (mock[Sink], mock[TaggedReducer], mock[FileSystems], mock[FileSystem])
//    val configuration = new ScoobiConfigurationImpl { override def fileSystem = files }
//    val mrj = new MapReduceJob(0) { override protected val fileSystems = fss  }
//
//    fss.listPaths(anyPath)(anySC) returns Seq(new Path("_SUCCESS"))
//    fss.moveTo(anyPath)(anySC) returns ((p: Path) => p.getName === "_SUCCESS")
//    // mock a sink for this job
//    sink.outputPath(anySC) returns Some(new Path("out"))
//
//    // collect outputs and check that files were moved
//    mrj.collectOutputs(configuration)(new Job)
//
//    there was one(fss).moveTo(===(new Path("out")))(anySC)
    Pending("must be reimplemented")
  }


  def anyPath = any[Path]
  def anySC   = any[ScoobiConfiguration]
}

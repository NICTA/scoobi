package com.nicta.scoobi
package core

import testing.TestFiles
import testing.mutable.UnitSpecification
import org.specs2.mock.Mockito
import impl.ScoobiConfigurationImpl
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import io.avro.AvroOutput._

class CheckpointSpec extends UnitSpecification with Mockito {
  "A checkpoint exists on a Sink if the sink is a checkpoint and if there are files with previous results in the output directory" >> {
    val mockFs = mock[FileSystem]
    implicit val configuration = new ScoobiConfigurationImpl() { override def fileSystem = mockFs }
    val checkpoint = avroSink[Int](TestFiles.createTempFile("test")(configuration).getPath, checkpoint = true)

    // the output path exists
    mockFs.exists(any[Path]) returns true
    // there are files in the output directory
    mockFs.listStatus(any[Path]) returns Array(new FileStatus())

    checkpoint.checkpointExists(configuration) must beTrue
  }
}

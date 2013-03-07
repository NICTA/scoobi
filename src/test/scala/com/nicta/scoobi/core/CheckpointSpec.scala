package com.nicta.scoobi
package core

import testing.TestFiles
import testing.mutable.UnitSpecification
import org.specs2.mock.Mockito
import io.text.TextFileSink
import impl.ScoobiConfigurationImpl
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import java.util
import java.io.File

class CheckpointSpec extends UnitSpecification with Mockito {
  "A checkpoint exists on a Sink if the sink is a checkpoint and if there are files with previous results in the output directory" >> {
    val mockFs = mock[FileSystem]
    val configuration = new ScoobiConfigurationImpl() { override def fileSystem = mockFs }
    val checkpoint = new TextFileSink[Int](TestFiles.createTempFile("test")(configuration).getPath).checkpoint(configuration)

    // the output path exists
    mockFs.exists(any[Path]) returns true
    // there are files in the output directory
    mockFs.listStatus(any[Path]) returns Array(new FileStatus())

    checkpoint.checkpointExists(configuration) must beTrue
  }
}

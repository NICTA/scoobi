/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

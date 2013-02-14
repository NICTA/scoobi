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
package io

import java.io.File
import org.specs2.specification.Scope
import org.apache.hadoop.fs.{FileStatus, Path}
import impl.ScoobiConfiguration
import impl.io.FileSystems
import testing.mutable.UnitSpecification
import org.specs2.mutable.Tables
import java.net.URI

class FileSystemsSpec extends UnitSpecification with Tables {
  "A local file can be compared to a list of files on the server to check if it is outdated" >> {
    implicit val sc = ScoobiConfiguration()

    "if it has the same name and same size, it is an old file" >> new fs {
      isOldFile(Seq(new Path("uploaded"))).apply(new File("uploaded")) must beTrue
    }
    "if it has the same name and not the same size, it is a new  file" >> new fs {
      uploadedLengthIs = 10

      isOldFile(Seq(new Path("uploaded"))).apply(new File("uploaded")) must beFalse
    }
    "otherwise it is a new file" >> new fs {
      isOldFile(Seq(new Path("uploaded"))).apply(new File("new")) must beFalse
    }
  }
  "2 file systems are the same if they have the same host and same scheme" >> {
    val nullString: String = null
    def uri(host: String, scheme: String) = new URI(scheme+"://"+host)

    "host1"    | "scheme1"  | "host2"    | "scheme2"  | "same?" |>
    "local"    ! "file"     ! "local"    ! "file"     ! true    |
    "local"    ! "hdfs"     ! "local"    ! "file"     ! false   |
    "local"    ! "file"     ! "cluster"  ! "file"     ! false   |
    nullString ! nullString ! nullString ! nullString ! true    |
    nullString ! "file"     ! "local"    ! "file"     ! false   | { (h1, s1, h2, s2, same) =>
      FileSystems.sameFileSystem(uri(h1, s1), uri(h2, s2)) === same
    }
  }

  trait fs extends FileSystems with Scope {
    var uploadedLengthIs = 0
    /** default file status for all test cases */
    override def fileStatus(path: Path)(implicit sc: core.ScoobiConfiguration) =
      new FileStatus(uploadedLengthIs, false, 0, 0, 0, 0, null, null, null, null)
  }
}

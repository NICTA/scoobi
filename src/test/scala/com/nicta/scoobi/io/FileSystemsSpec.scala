package com.nicta.scoobi
package io

import org.specs2.mutable.Specification
import org.apache.hadoop.fs.{FileStatus, Path}
import java.io.File
import application.ScoobiConfiguration
import org.specs2.specification.Scope

class FileSystemsSpec extends Specification {
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
  trait fs extends FileSystems with Scope {
    var uploadedLengthIs = 0
    /** default file status for all test cases */
    override def fileStatus(path: Path)(implicit sc: ScoobiConfiguration) =
      new FileStatus(uploadedLengthIs, false, 0, 0, 0, 0, null, null, null, null)
  }
}

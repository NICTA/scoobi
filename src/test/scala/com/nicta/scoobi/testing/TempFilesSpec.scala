package com.nicta.scoobi.testing

import org.specs2.mutable.Specification
import org.specs2.matcher.DataTables
import TempFiles._
import java.io.File

class TempFilesSpec extends Specification with DataTables with mutable.Unit {

  "A path can be calculated relatively to an existing directory" >> {
    "directory"     || "path"                            || "relative"                 |>
    "/var/temp/d1"  !! "/user/me/temp/d1/1/hello.txt"    !! "/var/temp/d1/1/hello.txt" | { (dir, path, relative) =>
      relativePath(new File(dir), path) === relative
    }
  }

}

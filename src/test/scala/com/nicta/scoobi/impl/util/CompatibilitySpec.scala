package com.nicta.scoobi.impl.util

import org.specs2.mutable.Specification
import java.io.File
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.impl.io.Files

class CompatibilitySpec extends Specification {
  skipAllUnless(Compatibility.useHadoop2)

  "call rename with 2 paths" >> {
    implicit val configuration = new Configuration

    Files.deletePath(new Path("target/test"))
    new File("target/test/out").mkdirs
    new File("target/test/1/y/m/d").mkdirs
    new File("target/test/1/y/m/d/01").createNewFile()

    val srcPath = new Path("target/test/1/y/m/d/")
    val destPath = new Path("target/test/out")

    Compatibility.rename(srcPath, destPath)
    //FileSystem.get(configuration).rename(srcPath, destPath)
  }

}

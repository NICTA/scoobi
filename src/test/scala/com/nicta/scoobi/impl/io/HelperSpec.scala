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
package impl
package io

import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.specs2.mutable.After
import testing.mutable.UnitSpecification
import control.Exceptions._

class HelperSpec extends UnitSpecification {
"IO Helper methods.".endp

  "Getting FileStatus on non-existent Path returns an empty Seq." in new CommonFS {
    Helper.getFileStatus(new Path("non-existent-path"))(conf) must_== Seq.empty
  }

  "Getting FileStatus on Path which points to a single file." in new CommonFS {
    val path = createSpecBasePath("singleFile")

    fs.createNewFile(path)

    Helper.getFileStatus(path) must_== Seq[FileStatus](path)
  }

  "Getting FileStatus on multiple files in single dir." in new CommonFS {
    val path = createSpecBasePath("multiFile")
    val path1 = new Path(path, "part1")
    val path2 = new Path(path, "part2")

    createEmptyFiles(path1, path2)

    Helper.getFileStatus(new Path(basePath, "multiFile")) must_== Seq[FileStatus](path1, path2)
  }

  "Getting FileStatus on file Glob." in new CommonFS {
    val path = createSpecBasePath("globDir1")
    val path1 = new Path(path, "1/sub1/part1")
    val path2 = new Path(path, "1/subdir2/part2")
    val path3 = new Path(path, "2/sub1/part1")
    val path4 = new Path(path, "3/subdir1/part4")

    createEmptyFiles(path1, path2, path3, path4)

    Helper.getFileStatus(new Path(basePath, "globDir1/*/*")) must_== Seq[FileStatus](path1, path2, path3, path4)
  }

  "Path exists check on file Glob" in new CommonFS {
    val path = createSpecBasePath("globDir2")
    val path1 = new Path(path, "1/sub1/part1")
    val path2 = new Path(path, "1/other2/part2")
    val path3 = new Path(path, "2/sub1/part1")
    val path4 = new Path(path, "3/other1/part4")

    createEmptyFiles(path1, path2, path3, path4)

    Helper.pathExists(new Path(basePath, "globDir2/*/*"))
  }

  "Expecting pathCheck to return false on a non-existant file" in new CommonFS {
    !Helper.pathExists(new Path(basePath, "some/non/existant/file"))
  }

  "Deleting a file through Helper" in new CommonFS {
    val path = new Path(createSpecBasePath("deletion"), "test")

    createEmptyFiles(path)
    val existed = pathExists(path)

    Helper.deletePath(path)
    existed && !pathExists(path)
  }

  "Get single file per dir" in new CommonFS {
    val input = Seq(createFileStatus("/base/"), createFileStatus("/base/a"),
                     createFileStatus("/base/a/file1", false), createFileStatus("/base/a/file2", false),
                     createFileStatus("/base/b"), createFileStatus("/base/b/file1", false),
                     createFileStatus("/base/c"))
    Helper.getSingleFilePerDir(input) must_== Set(new Path("/base/a/file1"), new Path("/base/b/file1"))
  }

  "Get non hidden files" in new CommonFS {
    val path = createSpecBasePath("hidden_files")
    val partFilePath = new Path(path, "part-m-00000")
    createEmptyFiles(new Path(path, "_SUCCESS"), new Path(path, ".hidden"), partFilePath)

    Helper.getSingleFilePerDir(path) must_== Set(partFilePath.getPath)
  }
}

trait CommonFS extends After {

  implicit val conf = new Configuration
  val fs = FileSystem.getLocal(conf)
  val basePath = new Path(conf.get("hadoop.tmp.dir"), "scoobi/HelperTest")
  var specPath = Option[Path](null)

  implicit def toFileStatus(path: Path): FileStatus = fs.getFileStatus(path)

  def createEmptyFiles(paths: Path*) = paths foreach { fs.createNewFile(_) }

  def pathExists(path: Path) = tryOrElse {
    FileSystem.get(path.toUri, conf).globStatus(new Path(path, "*")).length > 0
  }(false)

  def createSpecBasePath(child: String): Path = {
    specPath = Option(new Path(basePath, child))
    specPath.get
  }

  def createFileStatus(path: String, isDir: Boolean = true): FileStatus = new FileStatus(1,isDir,0,0,0,new Path(path))

  // delete the paths after the test has finished
  def after = specPath match {
    case Some(p) => fs.delete(p, true)
    case _ =>
  }
}
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
package acceptance

import com.nicta.scoobi.testing.{TestFiles, TempFiles}
import testing.mutable.NictaSimpleJobs
import Scoobi._
import TestFiles._

class TextFileSpec extends NictaSimpleJobs {
  "A text file that is saved with overwrite = true can be rewritten" >> { implicit sc: SC =>
    val list = DList(1, 2, 3)

    val path1 = path(TempFiles.createTempFilePath("path"))
    val l1 = list.toTextFile(path1, overwrite = false)
    l1.run // run once

    // an exception must be thrown the 2nd time
    val l2 = DList(1, 2, 3).toTextFile(path1, overwrite = false)
    l2.run must throwAn[Exception]

    val path2 = path(TempFiles.createTempFilePath("path"))
    val l3 = list.toTextFile(path2, overwrite = true)
    l3.run // run once

    // an exception must not be thrown the 2nd time
    val l4 = DList(1, 2, 3).toTextFile(path2, overwrite = true)
    l4.run must not(throwAn[Exception])
  }
}

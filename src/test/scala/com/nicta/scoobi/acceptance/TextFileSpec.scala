package com.nicta.scoobi
package acceptance

import testing.TempFiles
import testing.mutable.NictaSimpleJobs
import Scoobi._

class TextFileSpec extends NictaSimpleJobs {
  "A text file that is saved with overwrite = true can be rewritten" >> { implicit sc: SC =>
    val list = DList(1, 2, 3)

    val path1 = TempFiles.createTempFilePath("path")
    val l1 = list.toTextFile(path1, overwrite = false)
    l1.run // run once

    // an exception must be thrown the 2nd time
    val l2 = DList(1, 2, 3).toTextFile(path1, overwrite = false)
    l2.run must throwAn[Exception]

    val path2 = TempFiles.createTempFilePath("path")
    val l3 = list.toTextFile(path2, overwrite = true)
    l3.run // run once

    // an exception must not be thrown the 2nd time
    val l4 = DList(1, 2, 3).toTextFile(path2, overwrite = true)
    l4.run must not(throwAn[Exception])
  }
}

package com.nicta.scoobi
package acceptance

import testing.TempFiles
import testing.mutable.NictaSimpleJobs
import Scoobi._

class TextFileSpec extends NictaSimpleJobs {
  "A text file that is saved with overwrite = true can be rewritten" >> { implicit sc: SC =>
    val list = DList(1, 2, 3)
    val l1 = list.toTextFile(TempFiles.createTempFilePath("path"), overwrite = false)
    l1.run // run once

    // an exception must be thrown the 2nd time
    l1.run(sc.reset) must throwAn[Exception]

    val l2 = list.toTextFile(TempFiles.createTempFilePath("path"), overwrite = true)
    l2.run // run once

    // an exception must not be thrown the 2nd time
    l2.run(sc.reset) must not(throwAn[Exception])
  }
}

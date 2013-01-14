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
package exec

import org.specs2.mutable.Specification
import ChannelOutputFormat._
import org.apache.hadoop.fs.Path

class ChannelOutputFormatSpec extends Specification {
  "Channels determine result files for a given job run" >> {
    "_SUCCESS is a result file for any sink" >> {
      isResultFile(0, 0)(new Path("_SUCCESS"))
    }
    "ch1out2-r-00000 is a result file for a sink with tag 1 and index 2" >> {
      isResultFile(1, 2)(new Path("ch1out2-r-00000"))
    }
  }

}

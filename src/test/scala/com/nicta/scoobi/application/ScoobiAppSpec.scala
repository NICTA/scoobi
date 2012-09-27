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
package application

import testing.mutable.UnitSpecification
import org.specs2.mutable.Tables

class ScoobiAppSpec extends UnitSpecification with Tables{

  "Arguments from the command line must be parsed" >> {
    val app = new ScoobiApp { def run {} }
    // see issue 109
    val arguments = Array("-Dscoobi.mapreduce.reducers.max=200", "run-main", "example.MyApp", "--", "scoobi", "local")
    app.main(arguments)

    "all the arguments go to a 'commandLineArguments' variable" >> {
      app.commandLineArguments === arguments.toSeq
    }
    "the user arguments go to an 'args' variable" >> {
      app.args === Seq("run-main", "example.MyApp")
    }
    "the scoobi arguments go to a 'scoobiArgs' variable" >> {
      app.scoobiArgs === Seq("local")
    }
    "By defaults logs must be displayed, at the INFO level" >> {
      app.quiet aka "quiet" must beFalse
      app.level must_== "INFO"
    }
    "If 'quiet' is passed on the command line then there must be no logs" >> {
      val app = new ScoobiApp { def run {} }
      app.main(Array("scoobi", "local.quiet"))
      app.quiet aka "quiet" must beTrue
    }
  }

  "In a ScoobiApp the upload of dependent jars depends on the nolibjar arguments and on the content of the jar containing the main class (fat jar or not)" >> {
    "nolibjars" | "fat jar" | "upload" |>
    true        ! true      ! false    |
    true        ! false     ! false    |
    false       ! true      ! false    |
    false       ! false     ! true     | { (nolibjars, fatjar, upload) =>
      val app = new ScoobiApp {
        def run {}
        override def mainJarContainsDependencies = fatjar
      }
      app.main(Array("example.MyApp", "--", "scoobi", if (nolibjars) "nolibjars" else ""))
      app.upload must be_==(upload)
    }
  }
}

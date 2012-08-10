package com.nicta.scoobi
package application

import org.specs2.mutable.Specification

class ScoobiAppSpec extends Specification {

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
}

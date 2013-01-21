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

import org.specs2.mutable.Tables
import org.specs2.specification.Scope

import testing.mutable.UnitSpecification
import Scoobi._

class ScoobiAppSpec extends UnitSpecification with Tables {

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
    "By default, the job executes on the cluster" >> {
      "however if 'inmemory' is passed, it executes locally" >> new run {
        application.main(Array("scoobi", "inmemory"))
        inMemory must beTrue
        onLocal must beFalse
      }
      "however if 'local' is passed, it executes locally" >> new run {
        application.main(Array("scoobi", "local"))
        inMemory must beFalse
        onLocal must beTrue
      }
      "If the useconfdir argument is used, then the HADOOP_HOME variable is chosen to find the configuration files" >> new run {
        lazy val execution = application.main(Array("scoobi", "useconfdir"))
        execution must throwAn[Exception]("The HADOOP_HOME variable is must be set to access the configuration files")
      }
      trait run extends Scope { outer =>
        var inMemory  = false
        var onLocal   = false
        var onCluster = false
        val application = new ScoobiApp {
          def run() { }
          override def inMemory[T](t: =>T)(implicit configuration: ScoobiConfiguration)   = { outer.inMemory = true; t }
          override def onLocal[T] (t: =>T)(implicit configuration: ScoobiConfiguration)   = { outer.onLocal = true; t }
          override def onCluster[T] (t: =>T)(implicit configuration: ScoobiConfiguration) = { outer.onCluster = true; t }
          // simulate the non existence of the HADOOP_HOME variable to test the useconfdir argument
          override lazy val HADOOP_COMMAND = None
          override def get(name: String)    = None
          override def getEnv(name: String) = None
        }
      }
    }
    "The run method can be used to persist DLists and DObjects in a ScoobiApp" >> {
      "this code compiles" ==> {
        import Scoobi._ // this should work even in presence of the Scoobi import
        new ScoobiApp {
          def run() = run(DList(1, 2, 3))
        }; ok
      }
    }
  }

  tag("issue 163")
  "It is possible to set a different Scoobi temp directory on the ScoobiConfiguration before the job executes" >> {
    var workDir = "undefined"
    new ScoobiApp {
      configuration.setScoobiDir("shared-drive")
      def run { workDir = configuration.workingDir }
    }.main(Array())

    "the Scoobi temporary directory has been set" ==> { workDir must contain("shared-drive") }
  }

  tag("issue 166")
  "The default scoobi work directory should be /tmp/scoobi-${user.name}" >> {
    "the Scoobi temporary directory has been set" ==> { ScoobiConfiguration().scoobiDir must startWith("/tmp/scoobi-"+System.getProperty("user.name")) }
    "the Scoobi temporary directory ends with /"  ==> { ScoobiConfiguration().scoobiDir must endWith("/") }
  }

  "It is possible to set a specific job name that will be used to create the job working directory" >> {
    var workDir = "undefined"
    new ScoobiApp {
      configuration.jobNameIs("WordCountApplication")
      def run { workDir = configuration.workingDir }
    }.main(Array())

    "the Scoobi job name is used to create the working directory" ==> { workDir must contain("WordCountApplication") }
  }

  "The default mapred-site.xml file must be added as a default resource on the configuration file "+
  "as soon as we create a ScoobiConfiguration object" >> {
    ScoobiConfiguration().get("mapred.job.tracker") must not beNull
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

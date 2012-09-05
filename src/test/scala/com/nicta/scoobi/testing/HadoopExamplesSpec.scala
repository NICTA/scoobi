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
package testing

import org.apache.commons.logging.LogFactory
import application._
import org.specs2.mock.Mockito
import org.specs2.execute.Result
import org.specs2.matcher.ResultMatchers
import HadoopLogFactory._
import testing.mutable.{UnitSpecification => UnitSpec}
import org.specs2.main.Arguments
import org.specs2.mutable.Specification

class HadoopExamplesSpec extends UnitSpec with Mockito with ResultMatchers { isolated

  "the local context runs the examples locally only" >> {
    implicit val context = localExamples

    "successful local run" >> {
      runMustBeLocal
    }
    "with timing" >> {
      context.withTiming.example1.execute.expected must startWith ("Local execution time: ")
    }
  }
  "the cluster context runs the examples remotely only" >> {
    implicit val context = clusterExamples
    "successful cluster run" >> {
      runMustBeCluster
    }
    "with timing" >> {
      context.withTiming.example1.execute.expected must startWith ("Cluster execution time: ")
    }
  }
  "the localThenCluster context runs the examples" >> {
    implicit val context = localThenClusterExamples

    "locally first, then remotely if there is no failure" >> {
      "normal execution" >> {
        runMustBeLocalThenCluster
      }
      "with timing" >> {
        forall(Seq("Local execution time: ", "Cluster execution time: ")) { s =>
          context.withTiming.example1.execute.expected.split("\n").toSeq must containMatch(s)
        }
      }
    }
    "only locally if there is a failure" >> {
      "normal execution" >> {
        context.example2.execute
        there was one(context.mocked).runOnLocal(any[Result])
        there was no(context.mocked).runOnCluster(any[Result])
      }
      "with timing" >> {
        val result = context.withTiming.example2.execute.expected.split("\n").toSeq
        result must containMatch("Local execution time: ")
        result must not containMatch("Cluster execution time: ")
      }
    }
    step("checking the logs")
    "if verbose logging is enabled then the Log instance must not be NoOpLog" >> {
      context.withVerbose.example1.execute
      LogFactory.getLog("any").getClass.getSimpleName must not(be_==("NoOpLog"))
    }
    step(HadoopLogFactory.setLogFactory())
  }
  "arguments can be used to control the execution of examples locally or on the cluster" >> {
    "'cluster'    runs on the cluster only"      >> runMustBeCluster(examplesWithArguments("scoobi cluster"))
    "'local'      run locally only"              >> runMustBeLocal(examplesWithArguments("scoobi local"))
  }
  "arguments for scoobi can be passed from the command line" >> {
    localExamples.scoobiArgs must beEmpty
    examplesWithArguments("scoobi verbose").scoobiArgs === Seq("verbose")
  }
  "examples must be quiet by default" >> {
    localExamples.quiet must beTrue
  }
  "examples must be verbose if 'verbose' is passed on the command line" >> {
    hadoopSpec("verbose").quiet must beFalse
    hadoopSpec("verbose").level === INFO
    hadoopSpec("verbose").categories === ".*"
  }
  "the log level can be passed from the command line" >> {
    localExamples.extractLevel("verbose")         === INFO
    localExamples.extractLevel("verbose.info")    === INFO
    localExamples.extractLevel("verbose.warn")    === WARN
    localExamples.extractLevel("verbose.WARN")    === WARN
    localExamples.extractLevel("verbose.all")     === ALL
  }
  "the categories to show can be passed from the command line" >> {
    localExamples.extractCategories("verbose")                   === ".*"
    localExamples.extractCategories("verbose.info")              === ".*"
    localExamples.extractCategories("verbose.warn")              === ".*"
    localExamples.extractCategories("verbose.all")               === ".*"
    localExamples.extractCategories("verbose.TESTING")           === "TESTING"
    localExamples.extractCategories("verbose.all.TESTING")       === "TESTING"
    localExamples.extractCategories("verbose.all.[scoobi.Step]") === "scoobi.Step"
    localExamples.extractCategories("verbose.all.scoobi")        === "scoobi"
  }

  // various Hadoop Examples traits
  def localExamples            = new HadoopExamplesForTesting { override def context = local }
  def clusterExamples          = new HadoopExamplesForTesting { override def context = cluster }
  def localThenClusterExamples = new HadoopExamplesForTesting { override def context = localThenCluster }
  def examples(includeTag: String) = new HadoopExamplesForTesting {
    override lazy val arguments = include(includeTag)
  }
  def examplesWithArguments(commandline: String) = new HadoopExamplesForTesting {
    override lazy val arguments = Arguments(commandline)
  }

  def runMustBeLocal(implicit context: HadoopExamplesForTesting) = {
    context.example1.execute
    there was one(context.mocked).runOnLocal(any[Result])
    there was no(context.mocked).runOnCluster(any[Result])
  }
  def runMustBeCluster(implicit context: HadoopExamplesForTesting) = {
    context.example1.execute
    there was no(context.mocked).runOnLocal(any[Result])
    there was one(context.mocked).runOnCluster(any[Result])
  }
  def runMustBeLocalThenCluster(implicit context: HadoopExamplesForTesting) = {
    context.example1.execute
    there was one(context.mocked).runOnLocal(any[Result])
    there was one(context.mocked).runOnCluster(any[Result])
  }
  def noRun(implicit context: HadoopExamplesForTesting) = {
    context.example1.execute
    there was no(context.mocked).runOnLocal(any[Result])
    there was no(context.mocked).runOnCluster(any[Result])
  }

  def hadoopSpec(args: String*) = new HadoopExamples {
    override lazy val arguments = Arguments(("scoobi" +: args).mkString(" "))
    val fs = "fs"
    val jobTracker = "jobtracker"
  }
}

trait HadoopExamplesForTesting extends Specification with HadoopExamples with Mockito { outer =>
  val mocked = mock[HadoopExamples]
  override val fs = "fs"
  override val jobTracker = "jobtracker"
  var timing = false
  var verbose = false

  override def showTimes = timing
  override def quiet = !verbose

  def withTiming  = { timing = true; this }
  def withVerbose = { verbose = true; this }

  lazy val example1 = "ex1" >> { conf: ScoobiConfiguration =>
    ok
  }
  lazy val example2 = "ex2" >> { conf: ScoobiConfiguration =>
    ko
  }

  override def runOnLocal[T](t: =>T)   = {
    mocked.runOnLocal(t)
    t
  }
  override def runOnCluster[T](t: =>T) = {
    mocked.runOnCluster(t)
    t
  }

  override def configureForLocal(implicit conf: ScoobiConfiguration) = {
    setLogFactory()
    mocked.configureForLocal(conf)
    conf
  }
  override def configureForCluster(implicit conf: ScoobiConfiguration) = {
    setLogFactory()
    mocked.configureForCluster(conf)
    conf
  }
}


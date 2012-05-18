package com.nicta.scoobi.testing

import org.specs2.mock.Mockito
import com.nicta.scoobi.ScoobiConfiguration
import org.apache.commons.logging.LogFactory
import org.specs2.mutable.Specification
import org.specs2.execute.Result
import org.specs2.matcher.ResultMatchers

class HadoopExamplesSpec extends Specification with Mockito with mutable.Unit with ResultMatchers { isolated

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
        runMustBeLocal
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
    step(WithHadoopLogFactory.setLogFactory())
  }
  "tags can be used to control the execution of examples" >> {
    "'hadoop' runs locally, then on the cluster" >> runMustBeLocalThenCluster(examples("hadoop"))
    "'cluster'    runs on the cluster only"          >> runMustBeCluster(examples("cluster"))
    "'local'      run locally only"                  >> runMustBeLocal(examples("local"))
    "'unit'       no run, that's for unit tests"     >> noRun(examples("unit"))
  }

  def localExamples            = new HadoopExamplesForTesting { override def context = local }
  def clusterExamples          = new HadoopExamplesForTesting { override def context = cluster }
  def localThenClusterExamples = new HadoopExamplesForTesting { override def context = localThenCluster }

  def examples(includeTag: String) = new HadoopExamplesForTesting {
    override lazy val arguments = include(includeTag)
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
    there was no(context.mocked).runOnLocal(any[Result])
    there was one(context.mocked).runOnCluster(any[Result])
  }
  def noRun(implicit context: HadoopExamplesForTesting) = {
    context.example1.execute
    there was no(context.mocked).runOnLocal(any[Result])
    there was no(context.mocked).runOnCluster(any[Result])
  }

  trait HadoopExamplesForTesting extends HadoopExamples { outer =>
    val mocked = mock[HadoopExamples]
    val fs = "fs"
    val jobTracker = "jobtracker"
    var timing = false
    var verbose = false

    override def showTimes = timing
    override def quiet = !verbose

    def withTiming  = { timing = true; this }
    def withVerbose = { verbose = true; this }

    def example1 = "ex1" >> { conf: ScoobiConfiguration =>
      conf.getConfResourceAsInputStream("") // trigger some logs
      ok
    }
    def example2 = "ex2" >> { conf: ScoobiConfiguration =>
      conf.getConfResourceAsInputStream("") // trigger some logs
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

}

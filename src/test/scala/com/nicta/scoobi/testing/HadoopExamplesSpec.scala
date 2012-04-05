package com.nicta.scoobi.testing

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import com.nicta.scoobi.ScoobiConfiguration
import org.specs2.execute.Result

class HadoopExamplesSpec extends Specification with Mockito { isolated

  "the local context runs the examples locally only" >> {
    val context = localExamples

    "successful local run" >> {
      context.example1.execute
      there was one(context.mocked).runOnLocal(any[Result])
      there was no(context.mocked).runOnCluster(any[Result])
    }
    "with timing" >> {
      context.withTiming.example1.execute.expected must startWith ("Local execution time: ")
    }
  }
  "the cluster context runs the examples remotely only" >> {
    val context = clusterExamples
    "successful cluster run" >> {
      context.example1.execute
      there was no(context.mocked).runOnLocal(any[Result])
      there was one(context.mocked).runOnCluster(any[Result])
    }
    "with timing" >> {
      context.withTiming.example1.execute.expected must startWith ("Cluster execution time: ")
    }
  }
  "the localThenCluster context runs the examples" >> {
    val context = localThenClusterExamples

    "locally first, then remotely if there is no failure" >> {
      "normal execution" >> {
        context.example1.execute
        there was one(context.mocked).runOnLocal(any[Result])
        there was one(context.mocked).runOnCluster(any[Result])
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
  }

  def localExamples            = new HadoopExamplesForTesting { override def context = local }
  def clusterExamples          = new HadoopExamplesForTesting { override def context = cluster }
  def localThenClusterExamples = new HadoopExamplesForTesting { override def context = localThenCluster }

  trait HadoopExamplesForTesting extends HadoopExamples { outer =>
    val mocked = mock[HadoopExamples]
    val fs = "fs"
    val jobTracker = "jobtracker"
    var timing = false

    override def showTimes = timing

    def withTiming = { timing = true; this }

    def example1 = "ex1" >> { conf: ScoobiConfiguration => ok }
    def example2 = "ex2" >> { conf: ScoobiConfiguration => ko }

    override def runOnLocal[T](t: =>T)   = { mocked.runOnLocal(t); t }
    override def runOnCluster[T](t: =>T) = { mocked.runOnCluster(t); t }

    override def configureForLocal(implicit conf: ScoobiConfiguration) = {
      mocked.configureForLocal(conf)
      conf
    }
    override def configureForCluster(implicit conf: ScoobiConfiguration) = {
      mocked.configureForCluster(conf)
      conf
    }
  }

}

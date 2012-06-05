package com.nicta.scoobi.guide

class Testing extends ScoobiPage { def is = "Testing guide".title^
  """
### Introduction

Scoobi provides testing support to make your coding experience as productive as possible. It is tightly integrated with [specs2](http://specs2.org) out-of-the-box, but you can reuse the testing traits with your own favorite testing library.

### Using specs2

First of all you need to add the specs2 dependency to your build.sbt file (use the same version that [Scoobi is using](https://github.com/NICTA/scoobi/tree/${SCOOBI_BRANCH}/build.sbt))

#### Base specification

The abstract class `com.nicta.scoobi.testing.mutable.HadoopSpecification` is the base class for testing Hadoop jobs. Here's an example showing how to use it:

    import com.nicta.scoobi.testing.mutable._
    import com.nicta.scoobi.testing.HadoopHomeDefinedCluster

    class WordCountSpec extends HadoopSpecification {

      "Counting words frequencies must return the frequency for each word" >> { conf: ScoobiConfiguration =>
        // your Scoobi code goes here
      }

    }

This specification does several things for you:

  * it creates a new `ScoobiConfiguration` for each example so that you should be able to run your examples concurrently
  * this configuration is setup with the properties found in the `$HADOOP_HOME/conf` directory. If the `$HADOOP_HOME` variable is not set or the properties not found you will get an exception at runtime
  * because you are passed in the configuration you can change those properties if required
  * every example will be executed twice: once locally and once on the cluster if the local execution doesn't fail
  * all the logging is intercepted so as not to clutter the console output
  * before executing the examples, all the dependent jars, as defined by the sbt classpath, will be copied in a directory on the cluster (`~/libjars` by default). This upload is only done for missing jars on the cluster

#### Tailoring

You can change every step in the process above and create your own Specification trait with a different behavior:

  * the `fs` and `jobTracker` properties comes from the `Cluster` trait and you can override them with hardcoded values so that you don't depend on what's on your build server

  * you can change the execution context of the examples by overriding the `context` method and returning `local` or `cluster` instead of `localThenCluster` which is the default [specs2 context](http://etorreborre.github.com/specs2/guide/org.specs2.guide.Structure.html#Contexts). The same thing is achievable on the sbt command line by using the `exclude` argument: `test-only *WordCount* -- exclude cluster` will only run locally.

  * the directory for loading the jars is defined by the `libjarsDirectory` property which you can override. More generally you can change the loading and distribution of jars by overriding methods of the `LibJars` trait

#### Fine tuning

##### Implicit configuration

By default, all the examples of a specification are executed concurrently, which is why each example needs to be passed its own `ScoobiConfiguration` instance. If you prefer having a sequential execution (with the `sequential` specs2 argument) you can omit the explicit passing of the `ScoobiConfiguration` object:


    class WordCountSpec extends HadoopSpecification {
      sequential
      "Counting words frequencies must return the frequency for each word" >> {
        // your Scoobi code goes here
      }
    }

##### Cluster properties

If you only have one cluster for your testing you can hardcode the `fs` and `jobTracker` properties by overriding the corresponding methods:

    class WordCountSpec extends HadoopSpecification {
      override def fs         = "hdfs://svm-hadoop1.ssrg.nicta.com.au"
      override def jobTracker = "svm-hadoop1.ssrg.nicta.com.au:8021"
      ...
    }

This will be especially useful if you execute your specifications on a build server where Hadoop is not installed or configured.

##### Logging

By default Hadoop logs will not be shown in the console. However they are essential to debugging failed jobs. Here's how to display them:

 * show all logs: `test-only *WordCount* -- scoobi verbose` (you can also override the `quiet` method

 * show some log levels: `test-only *WordCount* -- scoobi verbose.warning` (you can also override the `level` method). The log levels are the ones from the Apache commons logging library: `ALL`, `FATAL`, `INFO`, `WARN`, `TRACE`

 * show some log categories: `test-only *WordCount* -- scoobi verbose.warning.(hadoop|scoobi)` will only display the log lines where the category matches `.*(hadoop|scoobi).*`. Note that you can visually separate this regular expression for log categories with brackets to help the reading: `test-only *WordCount* -- scoobi.verbose.warning.[(hadoop|scoobi)].times`

 * you can additionally show the execution times, locally and on the cluster: `test-only *WordCount* -- scoobi verbose.times` (or override the `showTimes` method)

 * finally you can combine those flags: `test-only *WordCount* -- scoobi verbose.warning.times`

##### Tags

You can use tags to run only locally or only on the cluster, with the mixed-in the `HadoopTags` trait. This trait automatically tags all your examples with `acceptance, local, cluster`.

Those tags can be called from the sbt command-line to control the execution of the specification:

 * `sbt>test-only -- include hadoop` only runs the `HadoopSpecification`s
 * `sbt>test-only *WordCount* -- include local` only runs `WordCountSpec` examples locally
 * `sbt>test-only *WordCount* -- include cluster` only runs `WordCountSpec` examples on the cluster

##### Type alias

Passing the configuration to each example is a bit verbose so you can use a type alias to shorten it:

    class WordCountSpec extends HadoopSpecification {
      type SC = ScoobiConfiguration

      "Counting words frequencies must return the frequency for each word" >> { conf: SC =>
        // your Scoobi code goes here
      }
    }

#### Simple jobs

The `HadoopSpecification` class allows to create any kind of job and execute them either locally or on the cluster. The `SimpleJobs` trait is an additional trait which you can use to:

 * write some strings to a temporary input text file and get back a `DList` representing this data
 * execute some transformations based on the `DList` API
 * get the results as a `Seq[String]` from an temporary output file

        "getting the size of words" >> { implicit c: SC =>
          fromInput("hello", "world").run { list: DList[String] => list.map(_.size) } must_== Seq("5", "5")
        }

`fromInput` creates a temporary file and a new `DList` from a `TextInput`. Then the `run` method executes transformations on the DList and retrieves the results. At the end of the tests the temporary files are deleted unless the `keepFiles` parameter is set:

    "getting the size of words" >> { implicit c: SC =>
      fromInput("hello", "world").keep.run { list: DList[String] => list.map(_.size) } must_== Seq("5", "5")
    }

 Other jobs might be slightly more complex and require inputs coming from several files:

    "Numbers can be partitioned into even and odd numbers" >> { implicit sc: SC =>
      val numbers = fromInput((1 to count).map(i => r.nextInt(count * 2).toString):_*).lines.map((_:String).toInt)
      val (evens, odds) = run(numbers.partition(_ % 2 == 0))

      forall(evens.map(_.toInt))(i => i must beEven)
      forall(odds.map(_.toInt))(i => i must beOdd)
    }

### Using your own

Some of the functionalities described above has been extracted into traits which you can reuse with your own test framework:

 * `HomeDefinedHadoopCluster` provides an implementation of the `Cluster` trait extracting the `fs` and `jobTracker` values from the configuration files

 * `LibJars` distributes the dependent jars to the cluster

 * `WithLocalHadoop` provides the `onLocal` method to execute Hadoop code locally. It also defines the `quiet` and `showTimes` methods to display log statement and/or execution times

 * `WithHadoop` extends the `WithLocalHadoop` with the `onCluster` method to execute a Hadoop job on the cluster
  """

}

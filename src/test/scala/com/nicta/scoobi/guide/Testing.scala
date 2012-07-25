package com.nicta.scoobi
package guide

class Testing extends ScoobiPage { def is = "Testing guide".title^
  """
### Introduction

Scoobi provides testing support to make your coding experience as productive as possible. It is tightly integrated with [specs2](http://specs2.org) out-of-the-box, but you can reuse the testing traits with your own favorite testing library.

### Using specs2

First of all you need to add the specs2 dependency to your build.sbt file (use the same version that [Scoobi is using](https://github.com/NICTA/scoobi/tree/${SCOOBI_BRANCH}/build.sbt))

#### Base specification

The abstract class `com.nicta.scoobi.testing.mutable.HadoopSpecification` is the base class for testing Hadoop jobs. Here's an example showing how to use it:

    import com.nicta.scoobi.testing.mutable._

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

  * the `fs` and `jobTracker` properties comes from the `application.Cluster` trait and you can override them with hardcoded values so that you don't depend on what's on your build server

  * you can change the execution context of the examples by overriding the `context` method and returning `local` or `cluster` instead of `localThenCluster` which is the default [specs2 context](http://etorreborre.github.com/specs2/guide/org.specs2.guide.Structure.html#Contexts). The same thing is achievable on the sbt command line by using the `exclude` argument: `test-only *WordCount* -- exclude cluster` will only run locally.

  * the directory for loading the jars is defined by the `libjarsDirectory` property which you can override. More generally you can change the loading and distribution of jars by overriding methods of the `application.LibJars` trait

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

The display of Hadoop and Scoobi logs can be controlled by passing command-line arguments. By default logs are turned off (contrary to a `ScoobiApp`) but they can be turned on by using the `verbose` arguments. See the [Application](${SCOOBI_GUIDE_PAGE}Application.html#Logging) in this User Guide to learn how to set log levels and log categories.

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

        class WordCountSpec extends HadoopSpecification with SimpleJobs {
          "getting the size of words" >> { implicit c: SC =>
            fromInput("hello", "world").run { list: DList[String] => list.map(_.size) } must_== Seq("5", "5")
          }
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

Some of the functionalities described above has been extracted into traits in the `application` package which you can reuse with your own test framework:

 * `ScoobiAppConfiguration` provides a `ScoobiConfiguration` configured from the `HADOOP_HOME/conf` files

 * `LocalHadoop` provides the `onLocal` method to execute Hadoop code locally. It also defines the `quiet` and `showTimes` methods to display log statement and/or execution times

 * `Hadoop` extends the `LocalHadoop` with the `onCluster` method to execute a Hadoop job on the cluster

 * `ScoobiUserArgs` parses command line arguments to extract meaningful values (`quiet`, `showTimes`,...) for the `Hadoop` trait

 * `LibJars` distributes the dependent jars to the cluster

  """

}

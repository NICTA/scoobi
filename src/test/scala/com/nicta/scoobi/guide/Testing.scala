package com.nicta.scoobi.guide

import org.specs2.Specification

class Testing extends Specification { def is = "Testing guide".title^
                                                                                                                        """
### Introduction

Scoobi provides testing support to make your coding experience as productive as possible. It is tightly integrated with [specs2](http://specs2.org) out-of-the-box, but you can reuse the testing traits with your own favorite testing library.

### Using specs2

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

  * you can show the logs by overriding the `quiet` method and set it to `true` (on the command line, you can pass: `test-only *WordCount* -- scoobi.verbose`)

  * you can show the execution times by overriding the `showTimes` method and set it to `true` (on the command line, you can pass: `test-only *WordCount* -- scoobi.times`)

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

### Using your own

Some of the functionalities described above has been extracted into traits which you can reuse with your own test framework:

 * `HomeDefinedHadoopCluster` provides an implementation of the `Cluster` trait extracting the `fs` and `jobTracker` values from the configuration files

 * `LibJars` distributes the dependent jars to the cluster

 * `WithLocalHadoop` provides the `onLocal` method to execute Hadoop code locally. It also defines the `quiet` and `showTimes` methods to display log statement and/or execution times

 * `WithHadoop` extends the `WithLocalHadoop` with the `onCluster` method to execute a Hadoop job on the cluster
                                                                                                                         """^
                                                                                                                        end

}

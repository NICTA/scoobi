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
package guide

class Testing extends ScoobiPage { def is = "Testing and debugging".title^
  """
### Running locally

The first thing to do is to run your code locally (in non-distributed mode).  This does not require you to be running on a Hadoop cluster or even to have Hadoop installed on your local machine.  The way to do that is simply to run your application normally under the JVM.  You can do that directly using `java`, but specifying the right class path is tricky; as a result, it's often easier to run using `sbt run-main`.  For example, if your main class is called `mypackage.myapp.RunMyApp`, run something like the following command from the top-level directory of your SBT project:

    sbt run-main mypackage.myapp.RunMyApp input-files output

### Running using Hadoop

Only when your application runs and works locally should you try running on a Hadoop cluster.  In order to to do you will probably need to build an assembly, i.e. a self-contained JAR file containing all of the libraries needed.  This can be done using the `sbt-scoobi` or `sbt-assembly` add-ons to SBT.  See the [Quick Start](${SCOOBI_GUIDE_PAGE}Quick%20Start.html) page in this User Guide for more information.

Once you have built the assembly, you can run it by logging into the job tracker and running it using `hadoop`, e.g. as follows:

    hadoop jar ./target/MyApp-assembly-0.1.jar mypackage.myapp.RunMyApp input-files output

### About input and output files

If your input files are text files, they can be compressed using GZIP (`.gz` or `.gzip` extension) or BZIP2 (`.bz2` or `.bzip2` extension), and Hadoop will automatically decompress them as it reads them.  However, GZIP files cannot be split by Hadoop (i.e. the entire file will need to be processed in a single map task), and BZIP2 files can only be split by Hadoop 2.x or later (or Hadoop 0.21 or later, in the beta series).

When running locally (in non-distributed mode), relative input and output files refer to the local file system.  When running on a Hadoop cluster, input and output files normally refer to HDFS.  However, you can refer to the local file system even in cluster mode by using the `file:///...` prefix, e.g. the following command

    hadoop jar ./target/MyApp-assembly-0.1.jar mypackage.myapp.RunMyApp file:///home/jdoe/data/input-files output

will read from the path `/home/jdoe/data/input-files` on the local file system, but write to the path `output` on HDFS.  Note that this will *only* work if your local files are mounted in the same place on all of the task tracker nodes and on the client.

### Objects are not shared across JVM's (and how to work around this)

When running locally, all of the code in a Scoobi application ends up running in the same JVM (Java virtual machine).  This is not the case when running under on a Hadoop cluster, where multiple processes running on multiple machines may be involved.  As a result, some care needs to be taken to ensure that data created in one function can be accessed in another.
 
The machine you run your Scoobi app on is termed the *client*.  Under Hadoop, this is typically (although not necessarily) the Hadoop *job tracker* -- the master machine that controls all the *task tracker* machines that implement the actual map and reduce jobs.  Your code continues to run on the client until you call `persist`.  The function calls that create `DList`s (e.g. `fromTextFile`) and operate on them (e.g. `map` or `filter`) occur on the client.  However, the operations implied by these function calls aren't actually performed at this time.  Rather a graph of operations (the *computational graph*) is constructed, and executed only when `persist` is called.  This graph consists of operations to perform, along with the *transformer* functions to implement these operations -- i.e. the functions that you specify as parameters to `map`, `filter` and other `DList` operations.

Once `persist` is called, Scoobi constructs and submits one or more MapReduce jobs, assigning each operation to either the map, shuffle or reduce component of a MapReduce job.  The map and reduce components of a given job are typically divided into a number of map and reduce *tasks*, each of which handles a portion of the input and is executed in its own JVM on one of the task trackers.  Scoobi arranges for the computational graph and related objects to be serialized to disk and shipped over to the various task trackers.

Each invocation of a transformer function is executed on one of the task trackers, and different invocations of a mapping or filtering function may run on different task trackers.  Thus, transformers will not have access to objects that were created in the client unless they were specifically shipped over as part of the serialization process, and will not have access to objects created in a different task tracker. (Although different invocations of a transformer *may* run on the same task tracker, there is no guarantee of this, and the particular invocations that are grouped together is likely to vary from run to run.)

The typical symptom of attempting to access an object created in a different JVM is a null pointer error, although in some cases more subtle problems can occur.

The ways to avoid this problem are:

1. Avoid using "global variables" in your code, i.e. variables stored as fields in a Scoobi `object`.  You can often work around this problem by declaring such variables as `lazy`, meaning that they won't be initialized until the first time they are accessed.  However, this will only work if the first access occurs inside a transformer (and only if the object is immutable).

2. To pass information from the client to a transformer, pass the information explicitly.  Either pass it as a parameter of the transformer function, or make the transformer a method and include the information in the object on which the transformer method is called.

3. Don't attempt to use shared mutable state to pass information between transformers (including between different invocations of the same transformer).  This is almost guaranteed to fail.

4. Be careful using external libraries.  Some libraries may violate the above rules under the hood, which is likely to cause problems.  You may need to rewrite the library (if you have access to the source code), or use a different library.


### Debugging large applications

Running and debugging large applications can be tricky.  Log files may be scattered across multiple task nodes, and errors that make a task fail, and in turn cause the entire job and application to fail, may occur after several hours after starting the application.  The following are some useful tips:

1. To allow some tasks to fail without the entire job failing, use the settings `mapred.max.map.failures.percent` and `mapred.max.reduce.failures.percent`.  For example, the following command

    hadoop jar ./target/MyApp-assembly-0.1.jar mypackage.myapp.RunMyApp -Dmapred.max.map.failures.percent=20 -Dmapred.max.reduce.failures.percent=20 input-files output

will allow up to 20% of map and reduce tasks to fail while still allowing the operation as a whole to proceed.  This is particularly important if your Scoobi application consists of multiple MapReduce jobs, because failure of one job will cause the entire application to stop running and fail, and by default a single failed task will trigger job failure. (Tasks are normally attempted 4 times before they are considered failed; however, this won't help if the failure is due to unexpected input, a bug in rarely encountered code, or some other problem that happens consistently, rather than a transient condition).

2. Scoobi logs additional debugging information at the `DEBUG` logging level.  Unfortunately, Apache Commons Logging provides no means of programmatically changing the logging level.  To do this you have to reach down directly into `log4j`, the underlying logging implementation used in Hadoop, as follows:

    import org.apache.log4j.{Level=>JLevel,_}

    {
      ...
      LogManager.getRootLogger().setLevel(JLevel.DEBUG.asInstanceOf[JLevel])
      ...
    }

### Using specs2

Scoobi provides testing support to make your coding experience as productive as possible. It is tightly integrated with [specs2](http://specs2.org) out-of-the-box, but you can reuse the testing traits with your own favorite testing library.

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
 * "run" the `DList` and get the results as a `Seq[T]`

        class WordCountSpec extends HadoopSpecification with SimpleJobs {
          "getting the size of words" >> { implicit c: SC =>
            val list: List[String] = fromInput("hello", "world")
            list.map(_.size).run must_== Seq("5", "5")
          }
        }

`fromInput` creates a temporary file and a new `DList` from a `TextInput`. Then the `run` method persists the DList and retrieves the results. At the end of the tests the temporary files are deleted unless the `keepFiles` argument is set on the command line

 Other jobs might be slightly more complex and require inputs coming from several files:

    "Numbers can be partitioned into even and odd numbers" >> { implicit sc: SC =>
      val numbers = fromInput((1 to count).map(i => r.nextInt(count * 2).toString):_*).map((_:String).toInt)
      val (evens, odds) = numbers.partition(_ % 2 == 0).run

      forall(evens.map(_.toInt))(i => i must beEven)
      forall(odds.map(_.toInt))(i => i must beOdd)
    }

### Using your own

Some of the functionalities described above has been extracted into traits in the `application` package which you can reuse with your own test framework:

 * `ScoobiAppConfiguration` provides a `ScoobiConfiguration` configured from the `HADOOP_HOME/conf` files

 * `LocalHadoop` provides the `onLocal` method to execute Hadoop code locally

 * `Hadoop` extends the `LocalHadoop` with the `onCluster` method to execute a Hadoop job on the cluster

 * `ScoobiUserArgs` parses command line arguments to extract meaningful values (`quiet`, `showTimes`,...) for the `Hadoop` trait

 * `LibJars` distributes the dependent jars to the cluster

  """

}

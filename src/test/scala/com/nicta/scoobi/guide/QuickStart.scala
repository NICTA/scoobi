package com.nicta.scoobi
package guide

class QuickStart extends ScoobiPage { def is = "Quick Start".title^
                                                                                                                        """
### Prerequisites

Before starting, you will need:

* [Cloudera's Hadoop 0.20.2 (CDH3)](http://www.cloudera.com/hadoop/)
* [Sbt 0.12.0](https://github.com/harrah/xsbt/wiki)

In addition to Hadoop, scoobi uses [sbt](https://github.com/harrah/xsbt/wiki) (version 0.12.0) to simplify building and packaging a project for running on Hadoop. We also provide an sbt plugin [sbt-scoobi](https://github.com/NICTA/sbt-scoobi) to allow you to contain a self-contained JAR for hadoop.
  
### Directory Structure  
  
Here the steps to get started on your own project:

```
$ mkdir my-app
$ cd my-app
$ mkdir -p src/main/scala
```

We first can create a `build.sbt` file that has a dependency on Scoobi:

```scala
name := "MyApp"

version := "0.1"

scalaVersion := "2.9.2"

libraryDependencies += "com.nicta" %% "scoobi" % "${SCOOBI_VERSION}"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

resolvers += "Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
```

### Write your code

Now we can write some code. In `src/main/scala/myfile.scala`, for instance:

```scala
package mypackage.myapp

import com.nicta.scoobi.Scoobi._

object WordCount extends ScoobiApp {
  def run() {
    val lines = fromTextFile(args(0))

    val counts = lines.flatMap(_.split(" "))
                      .map(word => (word, 1))
                      .groupByKey
                      .combine((a: Int, b: Int) => a + b)
    
    persist(toTextFile(counts, args(1)))
  }
}
```  

### Running

The Scoobi application can now be compiled and run using sbt:

```
> sbt compile
> sbt run-main mypackage.myapp.WordCount input-files output
```

Your Hadoop configuration will automatically get picked up, and all relevant JARs will be made available.

### Packaging

Often it is useful to construct a self-contatined executable JAR that can be fed directly into Hadoop. This is easy using the sbt-scoobi plugin. To use the sbt-scoobi plugin we need to create a file called `project/project/scoobi.scala` with the following contents:

```scala
import sbt._

object Plugins extends Build {
  lazy val root = Project("root", file(".")) dependsOn(
    uri("git://github.com/NICTA/sbt-scoobi.git#master")
  )
}
```

We also need to modify the library dependencies in the `build.sbt`:

```
libraryDependencies += "com.nicta" %% "scoobi" % "${SCOOBI_VERSION}"
```

The `provided` is added to the `scoobi` dependency to let sbt know that scoobi is provided by the sbt-plugin when it packages everything in a jar. If you don't included this `provided` nothing bad will happen, but the jar will contain some Scoobi dependencies that are not strictly required (e.g. Hadoop itself).

We can now use sbt to easily build and package our application into a self-contained executable JAR:

```
$ sbt package-hadoop # creates a self contained jar in target/Myapp-hadoop-version.jar
```  

The executable jar can now be run from the command line using the `hadoop` command:

```
# make sure to make args an input and output, if following along with the wordcount example
$ hadoop jar ./target/MyApp-app-hadoop-0.1.jar <args>
``` 
  
Note that there appears to be an OSX-specific [issue](https://github.com/NICTA/scoobi/issues/1) associated with calling `hadoop` in this manner requiring the jar to be added to `HADOOP_CLASSPATH` and then `hadoop` being given the correct object to run. e.g.:

```
$ export HADOOP_CLASSPATH=$PWD/target/MyApp-app-hadoop-0.1.jar
$ hadoop WordCount <args>
```

If you had any trouble following along, take a look at [Word Count](https://github.com/NICTA/scoobi/tree/${SCOOBI_BRANCH}/examples/wordCount) for a self contained example.                                                                                                                """
}

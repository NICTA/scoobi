package com.nicta.scoobi.guide

class QuickStart extends ScoobiPage { def is = "Quick Start".title^
                                                                                                                        """
### Prerequisites

Before starting, you will need:

* [Cloudera's Hadoop 0.20.2 (CDH3)](http://www.cloudera.com/hadoop/)
* [Sbt 0.11.3](https://github.com/harrah/xsbt/wiki)

In addition to Hadoop, scoobi uses [sbt](https://github.com/harrah/xsbt/wiki) (version 0.11.3) to simplify building and packaging a project for running on Hadoop. We also provide an sbt plugin [sbt-scoobi](https://github.com/NICTA/sbt-scoobi) to allow you to contain a self-contained jar for hadoop.
  
### Directory Structure  
  
Here the steps to get started on your own project:

```
  $ mkdir my-app
  $ cd my-app
  $ mkdir -p src/main/scala
  $ mkdir -p project/project
```

We first can create a `build.sbt` file that has a dependency on Scoobi:

```scala
  name := "MyApp"

  version := "0.1"

  scalaVersion := "2.9.2"

  libraryDependencies += "com.nicta" %% "scoobi" % "${SCOOBI_VERSION}" % "provided"

  scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

  resolvers ++= Seq("Cloudera Maven Repository" at "https://repository.cloudera.com/content/repositories/releases/",
                "Packaged Avro" at "http://nicta.github.com/scoobi/releases/")
```

To use the sbt-scoobi plugin we need to create a `project/project/scoobi.scala` file with the following contents:

```scala
  import sbt._

  object Plugins extends Build {
    lazy val root = Project("root", file(".")) dependsOn(
      uri("git://github.com/NICTA/sbt-scoobi.git#master")
    )
  }
```

The `provided` is added to the `scoobi` dependency to let sbt know that scoobi is provided by the sbt-plugin when it packages everything in a jar. If you don't included this `provided` nothing bad will happen, but the jar will contain some Scoobi dependencies that are not strictly required (e.g. Hadoop itself).

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
    }
  }
```  

### Packaging
  
We can now use sbt to easily build and package our application into a self-contained executable jar to feed directly into Hadoop:

```
  $ sbt package-hadoop # creates a self contained jar in target/Myapp-hadoop-version.jar
```  
  
### Running 

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

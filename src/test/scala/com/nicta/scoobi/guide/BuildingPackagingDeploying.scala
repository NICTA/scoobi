package com.nicta.scoobi.guide

class BuildingPackagingDeploying extends ScoobiPage { def is = "Deployment".title ^
                                                                                                                        """
### Sbt project

Scoobi projects are generally developed with [sbt](https://github.com/harrah/xsbt/wiki), and to simplify the task of building and packaging a project for running on Hadoop, it's really handy to use the sbt plugin [sbt-scoobi](https://github.com/NICTA/sbt-scoobi). Here are a few steps for creating a new project:

Create a new Scoobi application and add some code:

      $ mkdir my-app
      $ cd my-app
      $ mkdir -p src/main/scala
      $ vi src/main/scala/MyApp.scala

To use the sbt-scoobi plugin we need to include a `project/project/scoobi.scala` file with the following contents:

      import sbt._

      object Plugins extends Build {
        lazy val root = Project("root", file(".")) dependsOn(
          uri("git://github.com/NICTA/sbt-scoobi.git#master")
        )
      }

And, we can add a pretty standard `build.sbt` that has a dependency on Scoobi:

      name := "MyApp"

      version := "0.1"

      scalaVersion := "2.9.1"

      libraryDependencies += "com.nicta" %% "scoobi" % "${SCOOBI_VERSION}" % "provided"

      scalacOptions += "-deprecation"

The `provided` is added to the `scoobi` dependency to let sbt know that Scoobi is provided by the sbt-plugin when it packages everything in a jar. If you don't included this `provided` nothing bad will happen, but the jar will contain some Scoobi dependencies that are not strictly required.

We can now use sbt to easily build and package our application into a self-contained executable jar to feed directly into Hadoop:

      $ sbt package-hadoop
      $ hadoop jar ./target/MyApp-app-hadoop-0.1.jar <args>

Note that there appears to be a OSX-specific [issue](https://github.com/NICTA/scoobi/issues/1) associated with calling `hadoop` in this manner requiring the jar to be added to `HADOOP_CLASSPATH`
and then `hadoop` being given the correct object to run. e.g.:

      $ export HADOOP_CLASSPATH=$PWD/target/Scoobi_Word_Count-hadoop-0.1.jar
      $ hadoop WordCount inputFile/to/wordcount nonexistent/outputdir

"""

}

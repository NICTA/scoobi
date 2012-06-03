
Creating a Scoobi project with sbt
----------------------------------

Scoobi projects are generally developed with [sbt](https://github.com/harrah/xsbt/wiki), and to simplify
the task of building and packaging a project for running on Hadoop, it's really handy to use the sbt plugin
[sbt-scoobi](https://github.com/NICTA/sbt-scoobi). Here are a few steps for creating a new project:

Create a new Scoobi application and add some code:

```
    $ mkdir my-app
    $ cd my-app
    $ mkdir -p src/main/scala
    $ vi src/main/scala/MyApp.scala
```

To use the sbt-scoobi plugin we need to include a `project/project/scoobi.scala` file with the following contents:

```scala
    import sbt._

    object Plugins extends Build {
      lazy val root = Project("root", file(".")) dependsOn(
        uri("git://github.com/NICTA/sbt-scoobi.git#master")
      )
    }
```

And, we can add a `build.sbt` that has a dependency on Scoobi:

```scala
    name := "MyApp"

    version := "0.1"

    scalaVersion := "2.9.2"

    libraryDependencies += "com.nicta" %% "scoobi" % "0.4.0-SNAPSHOT" % "provided"

    scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")
    
    resolvers += "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
```

The `provided` is added to the `scoobi` dependency to let sbt know that Scoobi
is provided by the sbt-plugin when it packages everything in a jar. If you
don't included this `provided` nothing bad will happen, but the jar will contain
some Scoobi dependencies that are not strictly required.

We can now use sbt to easily build and package our application into a self-contained executable
jar to feed directly into Hadoop:

```
    $ sbt package-hadoop
    $ hadoop jar ./target/MyApp-app-hadoop-0.1.jar <args>
```

Note that there appears to be an OSX-specific [issue](https://github.com/NICTA/scoobi/issues/1)
associated with calling `hadoop` in this manner requiring the jar to be added to `HADOOP_CLASSPATH`
and then `hadoop` being given the correct object to run. e.g.:

```
    $ export HADOOP_CLASSPATH=$PWD/target/Scoobi_Word_Count-hadoop-0.1.jar
    $ hadoop WordCount inputFile/to/wordcount nonexistent/outputdir
```


Issues, questions and contributions
-----------------------------------

Scoobi is released under the Apache license v2. We welcome contributions of bug fixes and/or new
features via GitHib pull requests. In addition, it is important to us to build a friendly user
and developer community around Scoobi, so:

* If you happen to encounter something that looks suspiciously like a bug, be sure to log it on the
[GitHub issue tracker](https://github.com/NICTA/scoobi/issues) so that it can be fixed for everyone - the
more information the better;
* If, on the other hand, you simply have questions about how to use Scoobi, take a look at the posts on the
[scoobi-users](http://groups.google.com/group/scoobi-users) mailing list or post a question of your own;
* And, if you're keen to get your hands dirty and contribute new features to Scoobi, or are hoping to get some
insight into Scoobi's internal architecture, or simply want to know what's going on in developer-land, head
over to the [scoobi-dev](http://groups.google.com/group/scoobi-dev) mailing list.

We will try our best to respond to all issues and questions quickly.


Release notes
-------------------
[Changes in 0.3.0](https://github.com/nicta/scoobi/blob/master/CHANGES.md)

### Next Milestone
[0.4.0 Open issues](https://github.com/NICTA/scoobi/issues?milestone=2)

[0.4.0 Closed issues](https://github.com/NICTA/scoobi/issues?milestone=2&state=closed)

package com.nicta.scoobi
package guide

class Deployment extends ScoobiPage { def is = "Deployment".title^
                                                                                                                        """
### Introduction

If using `sbt run` to launch the job is not acceptable, it's possible to make self-contained fat-jars for deployment. Previously we had a plugin `sbt-scoobi` but since deprecated in favor of using existing tools. [Sbt-assembly](https://github.com/sbt/sbt-assembly/) does the trick, but unfortunately isn't too particularly easy/nice to use.


### Disable jars uploading

By default an application using th `ScoobiApp` trait, will upload all the dependent jars to the cluster in a `libjars` directory (see the [Application](Application.html#Dependencies) page). If you package your own fat jar with all the dependent classes, you will want to deactivate this functionality by adding the following arguments to the command line: `scoobi nolibjars`. Or alternatively, hardcode it in via `override def upload = false` inside your scoobi app.

### Sbt-assembly

First check [sbt-assembly](https://github.com/sbt/sbt-assembly/) for up to date information and documentation. This should be your first reference point. However, the way we have found works best /and please let us know if you find a better way/ is:

First add `sbt-assembly` plugin, at `project/plugins.sbt` with

```
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.3")
```

And now at the top of your `build.sbt` add the required incantation:

```
import AssemblyKeys._

assemblySettings
```

### Quick Hack

Ben Wing's suggestion provides a quick and easy solution by using a merge strategy, by putting this at the bottom of your `build.sbt`:

```
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case x => {
      val oldstrat = old(x)
      if (oldstrat == MergeStrategy.deduplicate) MergeStrategy.first
      else oldstrat
    }
  }
}
```

The downside is, that this brings in dependencies that are not strictly required (avro, hadoop, xmlpull etc.).

The big advantage to this approach, is that `sbt run` still works

### Ugly Hack

We can also fake the `libraryDependencies` to build a proper jar. Please note, the way we change this, will make it incompatible with a normal `sbt run`. So you might want to have two versions of your `build.sbt`

First of all, we want to only get some of scoobi's dependencies (namely we don't want Hadoop itself, likely don't need avro, and anything else that would trick up sbt-assembly). So what can use is a little trick:

```
"com.nicta" %% "scoobi" % "${SCOOBI_VERSION}" intransitive()
```

However, we do need some of Scoobi's dependencies -- so we have to add them in manually. Check out Scoobi's [build.sbt](https://github.com/NICTA/scoobi/blob/${SCOOBI_BRANCH}/build.sbt) (and the correct branch/revision) for the canonical list, but for example it might be:

```
"javassist" % "javassist" % "3.12.1.GA"
"org.apache.avro" % "avro-mapred" % "1.7.0" // Note: you only need this if you use it
"org.apache.avro" % "avro" % "1.7.0"        // Note: you only need this if you use it
"org.scalaz" %% "scalaz-core" % "6.95"
"com.thoughtworks.xstream" % "xstream" % "1.4.3" intransitive()
```

And lastly, we probably want `sbt compile` to work -- so we add in we add in all the dependencies we excluded, but as a provided". e.g.

"org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.0" % "provided"
"org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.0.0" % "provided"

When you put this all together, here's is what an example `build.sbt` should look like:

```
import AssemblyKeys._

assemblySettings

name := "Scoobi Word Count"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

libraryDependencies ++= Seq(
   "com.nicta" %% "scoobi" % "${SCOOBI_VERSION}" intransitive(),
   "javassist" % "javassist" % "3.12.1.GA",
   "org.apache.avro" % "avro-mapred" % "1.7.0", // Note: add ' % "provided"'  if you don't need it 
   "org.apache.avro" % "avro" % "1.7.0",        // Note: add ' % "provided"'  if you don't need it 
   "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.0" % "provided",
   "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.0.0" % "provided",
   "org.scalaz" %% "scalaz-core" % "6.95",
   "com.thoughtworks.xstream" % "xstream" % "1.4.3" intransitive()
   )

resolvers ++= Seq("cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "apache"   at "https://repository.apache.org/content/repositories/releases",
                  "scoobi"   at "http://nicta.github.com/scoobi/releases") // for scalaz 6.95
```

### Running

Create the jar:

```
$ sbt assembly
```

Running the jar:

```
$ hadoop jar target/appname-assembly-version.jar scoobi nolibjars
```


Note that there appears to be an OSX and Java 6 specific [issue](https://github.com/NICTA/scoobi/issues/1) associated with calling `hadoop` in this manner requiring the jar to be added to `HADOOP_CLASSPATH` and then `hadoop` being given the correct object to run. e.g.:

```
$ export HADOOP_CLASSPATH=$PWD/target/appname-assembly-version.jar
$ hadoop WordCount <args>
```

If you have any `ClassNotFoundException` with a Scoobi class missing, make sure you've disabled jars uploading (see above).
"""
}

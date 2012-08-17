package com.nicta.scoobi
package guide

class Deployment extends ScoobiPage { def is = "Deployment".title^
                                                                                                                        """
### Introduction

If using `sbt run` to launch the job is not acceptable, it's possible to make self-contained fat-jars for deployment. Previously we had a plugin `sbt-scoobi` but since deprecated in favor of using existing tools. [Sbt-assembly](https://github.com/sbt/sbt-assembly/) does the trick, but unfortunately isn't too particularly easy/nice to use.


### Disable Upload Jar

TODO: Document when it's done...

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

First of all, we want to not get all of scoobi's dependencies (as we don't want Hadoop itself, and because sbt-assembly can't handle some of merge conflicts). So what can use is a little trick:

```
"com.nicta" %% "scoobi" % "${SCOOBI_VERSION}" intransitive()
```

However, we do need some of Scoobi's dependencies -- so we have to add them in manually. Check out Scoobi's [build.sbt](https://github.com/NICTA/scoobi/blob/${SCOOBI_BRANCH}/build.sbt) (and the correct branch/revision) for the canonical list, but for example it might be:

```
"javassist" % "javassist" % "3.12.1.GA"
"org.apache.avro" % "avro-mapred" % "1.7.0" # Note: you only need this if you use it 
"org.apache.avro" % "avro" % "1.7.0"        # Note: you only need this if you use it
"org.scalaz" %% "scalaz-core" % "6.95"
"com.thoughtworks.xstream" % "xstream" % "1.4.3"
```

but unfortunately `sbt-assembly` (currently) can't handle xstream very well. So we can manually force it to work with the same trick:

```
"com.thoughtworks.xstream" % "xstream" % "1.4.3" intransitive()
```

and manually adding xstream's required dependency:

```
"xpp3" % "xpp3" % "1.1.4c"
```


When you put this all together, here's is what an example `build.sbt` should look like:


```
import AssemblyKeys._

assemblySettings

name := "Scoobi Word Count"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

libraryDependencies ++= Seq(
	"com.nicta" %% "scoobi" % "0.5.0-SNAPSHOT" intransitive(),
	"javassist" % "javassist" % "3.12.1.GA",
	"org.apache.avro" % "avro-mapred" % "1.7.0",  # Note: you only need this if you use it
	"org.apache.avro" % "avro" % "1.7.0",         # Note: you only need this if you use it
	"org.scalaz" %% "scalaz-core" % "6.95",
	"com.thoughtworks.xstream" % "xstream" % "1.4.3" intransitive(),
	"xpp3" % "xpp3" % "1.1.4c"
	)

resolvers += "Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
```

### Running

Create the jar:

```
$ sbt assembly
```

Running the jar:

```
$ hadoop jar target/appname-assembly-version.jar
```


Note that there appears to be an OSX and Java 6 specific [issue](https://github.com/NICTA/scoobi/issues/1) associated with calling `hadoop` in this manner requiring the jar to be added to `HADOOP_CLASSPATH` and then `hadoop` being given the correct object to run. e.g.:

```
$ export HADOOP_CLASSPATH=$PWD/target/appname-assembly-version.jar
$ hadoop WordCount <args>
```

TODO: Document command line argument to disable upload jar when its done"""
}

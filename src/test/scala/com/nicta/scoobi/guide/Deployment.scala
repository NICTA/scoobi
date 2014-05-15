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

class Deployment extends ScoobiPage { def is = "Deployment".title^
  s2"""
### Introduction

If using `sbt run` to launch the job is not acceptable, it's possible to make self-contained fat-jars for deployment with:

 - [Sbt assembly](https://github.com/sbt/sbt-assembly)
 - [Maven assembly](http://maven.apache.org/plugins/maven-assembly-plugin/usage.html)

### Sbt-assembly

First check [sbt-assembly](https://github.com/sbt/sbt-assembly/) for up to date information and documentation. This should be your first reference point. However, the way we have found works best /and please let us know if you find a better way/ is:

First add `sbt-assembly` plugin, at `project/plugins.sbt` with

```
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.10.2")
```

And now at the top of your `build.sbt` add the required incantation:

```
import AssemblyKeys._

assemblySettings
```

#### Quick Hack

Ben Wing's suggestion provides a quick and easy solution by using a merge strategy, by putting this at the bottom of your `build.sbt`:

```
mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
 case entry => {
   val strategy = mergeStrategy(entry)
   if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
   else strategy
 }
}}
```

The downside is, that this brings in dependencies that are not strictly required (avro, hadoop, etc...).

The big advantage to this approach, is that `sbt run` still works

#### Ugly Hack

We can also fake the `libraryDependencies` to build a proper jar. Please note, the way we change this, will make it incompatible with a normal `sbt run`. So you might want to have two versions of your `build.sbt`

First of all, we want to only get some of scoobi's dependencies (namely we don't want Hadoop itself, likely don't need avro, and anything else that would trick up sbt-assembly). So what can use is a little trick:

```
"com.nicta" %% "scoobi" % "$VERSION" intransitive()
```

However, we do need some of Scoobi's dependencies -- so we have to add them in manually. Check out Scoobi's [build.sbt](https://github.com/NICTA/scoobi/blob/$BRANCH/project/dependencies.scala) (and the correct branch/revision) for the canonical list, but for example it might be:

```
"org.scala-lang"                    %  "scala-compiler"            % "2.10.3",
"org.apache.avro"                   %  "avro"                      % "1.7.4",       // Note: you only need this if you use it
"com.thoughtworks.xstream"          %  "xstream"                   % "1.4.4"        intransitive(),
"javassist"                         %  "javassist"                 % "3.12.1.GA",
"com.googlecode.kiama"              %% "kiama"                     % "1.6.0-SNAPSHOT",
"com.chuusai"                       %  "shapeless_2.10.2"          % "2.0.0-M1",
```

And lastly, we probably want `sbt compile` to work -- so we add in all the dependencies we excluded, but as a provided". e.g.

```
"org.apache.hadoop" % "hadoop-common"                     % "2.2.0" % "provided",
"org.apache.hadoop" % "hadoop-hdfs"                       % "2.2.0" % "provided",
"org.apache.hadoop" % "hadoop-mapreduce-client-app"       % "2.2.0" % "provided",
"org.apache.hadoop" % "hadoop-mapreduce-client-core"      % "2.2.0" % "provided",
"org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.2.0" % "provided",
"org.apache.hadoop" % "hadoop-mapreduce-client-core"      % "2.2.0" % "provided",
"org.apache.hadoop" % "hadoop-annotations"                % "2.2.0" % "provided",
"org.apache.avro"   % "avro-mapred"                       % "1.7.4"   classifier "hadoop2", // Note: add ' % "provided"'  if you don't need it
```

When you put this all together, here's is what an example `build.sbt` should look like:

```
import AssemblyKeys._

assemblySettings

name := "Scoobi Word Count"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
    "com.nicta"                %% "scoobi"                           % "$VERSION" intransitive(),
    "org.apache.hadoop"        % "hadoop-common"                     % "2.2.0" % "provided",
    "org.apache.hadoop"        % "hadoop-hdfs"                       % "2.2.0" % "provided",
    "org.apache.hadoop"        % "hadoop-mapreduce-client-app"       % "2.2.0" % "provided",
    "org.apache.hadoop"        % "hadoop-mapreduce-client-core"      % "2.2.0" % "provided",
    "org.apache.hadoop"        % "hadoop-mapreduce-client-jobclient" % "2.2.0" % "provided",
    "org.apache.hadoop"        % "hadoop-mapreduce-client-core"      % "2.2.0" % "provided",
    "org.apache.hadoop"        % "hadoop-annotations"                % "2.2.0" % "provided",
    "org.apache.avro"          % "avro-mapred"                       % "1.7.4"    classifier "hadoop2", // Note: add ' % "provided"'  if you don't need it

    "org.scala-lang"           %  "scala-compiler"                   % "2.10.3",
    "org.scalaz"               %% "scalaz-core"                      % "7.0.2",
    "com.thoughtworks.xstream" %  "xstream"                          % "1.4.4"    intransitive(),
    "javassist"                %  "javassist"                        % "3.12.1.GA",
    "com.googlecode.kiama"     %% "kiama"                            % "1.6.0-SNAPSHOT",
    "com.chuusai"              %  "shapeless_2.10.2"                 % "2.0.0-M1",
  )

resolvers ++= Seq(Resolver.sonatypeRepo("releases"),
                  Resolver.sonatypeRepo("snaspshots"))
```

### Other Hadoop versions

Scoobi can also be used with [CDH3 and CDH4](http://bit.ly/1k25bes). In this case you need to use the following dependencies (note the name of the Scoobi jar):

```
// FOR CDH3
libraryDependencies ++= Seq(
    "com.nicta"                %% "scoobi"            % "$VERSION-cdh3" intransitive(),
    "org.apache.hadoop"        %  "hadoop-core"       % "0.20.2-cdh3u1" % "provided",
    "org.apache.avro"          %  "avro-mapred"       % "1.7.4",
    "org.scala-lang"           %  "scala-compiler"    % "2.10.3",
    "com.thoughtworks.xstream" %  "xstream"           % "1.4.4"        intransitive(),
    "javassist"                %  "javassist"         % "3.12.1.GA",
    "com.googlecode.kiama"     %% "kiama"             % "1.6.0-SNAPSHOT",
    "com.chuusai"              %  "shapeless_2.10.2"  % "2.0.0-M1",
  )

// FOR CDH4
libraryDependencies ++= Seq(
    "com.nicta"                %% "scoobi"            % "$VERSION-cdh4" intransitive(),
    "org.apache.hadoop"        %  "hadoop-client"     % "2.0.0-mr1-cdh4.0.1" exclude("asm", "asm") % "provided",
    "org.apache.hadoop"        %  "hadoop-core"       % "2.0.0-mr1-cdh4.0.1",                      % "provided",
    "org.apache.avro"          %  "avro-mapred"       % "1.7.4"              classifier "hadoop2",
    "org.scala-lang"           %  "scala-compiler"    % "2.10.3",
    "com.thoughtworks.xstream" %  "xstream"           % "1.4.4"              intransitive(),
    "javassist"                %  "javassist"         % "3.12.1.GA",
    "com.googlecode.kiama"     %% "kiama"             % "1.6.0-SNAPSHOT",
    "com.chuusai"              %  "shapeless_2.10.2"  % "2.0.0-M1"
  )


resolvers ++= Seq(Resolver.sonatypeRepo("releases"),
                  Resolver.sonatypeRepo("snaspshots"),
                  "cloudera"             at "https://repository.cloudera.com/content/repositories/releases")
```

### Maven assembly

Building Maven jars for Hadoop is documented in several blog posts. [This one](http://www.tikalk.com/java/build-your-first-hadoop-project-maven), for example, provides a step-by-step guide.

### Running

Running the jar:

```
$$ hadoop jar target/appname-assembly-version.jar scoobi
```

Note that there appears to be an OSX and Java 6 specific [issue](https://github.com/NICTA/scoobi/issues/1) associated with calling `hadoop` in this manner requiring the jar to be added to `HADOOP_CLASSPATH` and then `hadoop` being given the correct object to run. e.g.:

```
$$ export HADOOP_CLASSPATH=$$PWD/target/appname-assembly-version.jar
$$ hadoop WordCount <args>
```

### Troubleshooting

You might get some `ClassNotFoundException` on the cluster if Scoobi fails to recognise that your jar contains all your dependencies (as opposed to running from sbt for example). In that case you can force the dependencies to be included in the Scoobi "job jar" that is distributed to the cluster node by turning off the dependency uploading mechanism (see the [Application](${GUIDE_PAGE}com.nicta.scoobi.guide.Application.html) page):

`$$ hadoop WordCount <args> scoobi nolibjars`

  """
}

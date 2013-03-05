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

class QuickStart extends ScoobiPage { def is = "Quick Start".title^
                                                                                                                        """
### Prerequisites

Before starting, you will need:

* [Cloudera's Hadoop 4.0.1 (CDH4)](http://www.cloudera.com/hadoop/)
* [Sbt 0.12.1](http://www.scala-sbt.org/)

In addition to Hadoop, scoobi uses [sbt](http://www.scala-sbt.org/) (version 0.12.1) to simplify building and packaging a project for running on Hadoop.
  
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

resolvers ++= Seq(
    "nicta's avro" at "http://nicta.github.com/scoobi/releases",
    "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
    "Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots")
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
    
    persist(counts.toTextFile(args(1))
  }
}
```  

### Running

The Scoobi application can now be compiled and run using sbt:

```
> sbt compile
> sbt run-main 'mypackage.myapp.WordCount input-files output'
```

Your Hadoop configuration will automatically get picked up, and all relevant JARs will be made available.

If you had any trouble following along, take a look at [Word Count](https://github.com/NICTA/scoobi/tree/${SCOOBI_BRANCH}/examples/wordCount) for a self contained example."""
}

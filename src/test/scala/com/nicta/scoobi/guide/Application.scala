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

class Application extends ScoobiPage { def is = "Application".title^
  """
### Creation

Scoobi provides several traits to help building applications. `ScoobiApp` brings them all together:

```scala
object WordCount extends ScoobiApp {
  def run() {
    val lines = DList(repeat("hello" -> 3, "world" -> 4):_*)

    val frequencies = lines.flatMap(_.split(" "))
      .map(word => (word, 1))
      .groupByKey
      .combine((a: Int, b: Int) => a + b).materialize

    println(persist(frequencies).toList.sorted)
  }

  /** @return a Seq of strings where each key has been duplicated a number of times indicated by the value */
  def repeat(m: (String, Int)*): Seq[String] = m.flatMap { case (k, v) => Seq.fill(v)(k) }
}
```

When you extend the `ScoobiApp` trait:

  * you need to implement the `run` method with your Scoobi logic

  * if you create an object, you get a `main(arguments: Array[String])` method to run your code from the command-line: `run-main examples.WordCount`

  * you inherit the command-line arguments in a variable named `args`

  * you inherit an implicit `ScoobiConfiguration` object, for persisting `DLists`. The `ScoobiConfiguration` object encapsulates a Hadoop `Configuration` object setup with the hadoop command-line arguments

### Dependencies

The `ScoobiApp` trait extends the `LibJars` trait which provides functionalities to manage dependent jars. More specifically it helps you load all the dependent jars on the cluster before executing your Scoobi job:

 1. the dependent jars are taken from the context classloader and are only loaded if they come from a `.ivy2` or `.m2` directory (the Scala jars are *not* included). This behavior can be redefined by overriding the `jars` method

 2. `uploadLibJars` uploads all the new jars to a directory on the cluster. By default this directory is named `libjars` (this is declared in the `libJarsDirectory` method). When you upload the jars, *all* the dependencies will be added to the `mapred.classpath` Hadoop configuration variable)

 3. `deleteJars` can be used to remove all existing jars in the `libjars` directory if one of your dependencies has changed (but kept the same name)

Here is an example on how to use these methods:

```scala
object WordCount extends ScoobiApp {
  def run() {
    // remove the existing jars on the cluster
    deleteJars
    // upload the dependencies to the cluster
    uploadLibJars

    // define and execute a Scoobi job
    val lines = ...
  }
}
```

If you don't want to use this facility at all, you can override the `upload` method and set it to `false`.

### Configuration

The `ScoobiApp` trait uses the configuration files found in the `HADOOP_HOME` directory to determine the location of the cluster file system and job tracker. You can however modify this by:

 * passing the `scoobi local` argument on the command line
 * overriding the `locally` method inherited from the `ScoobiArgs` trait

 * passing other addresses via the `fs` and `jt` Hadoop arguments
 * overriding the `fs` and `jobTracker` methods inherited from the `Cluster` trait

### Logging

By default, when extending the `Hadoop` trait, Hadoop and Scoobi logs will be shown in the console at the "INFO" level. However for debugging failed jobs you may want to change the log level or the log categories:

 * show some log levels: `run-main WordCount -- scoobi warning` (you can also override the `level` method). The log levels are the ones from the Apache commons logging library: `ALL`, `FATAL`, `INFO`, `WARN`, `TRACE`

 * show some log categories: `run-main WordCount -- scoobi warning.(hadoop|scoobi)` will only display the log lines where the category matches `.*(hadoop|scoobi).*`. Note that you can visually separate this regular expression for log categories with brackets to help the reading: `run WordCount -- scoobi.warning.[(hadoop|scoobi)].times`

 * you can additionally show the execution times, locally and on the cluster: `run-main WordCount -- scoobi times` (or override the `showTimes` method)

 * finally you can combine those flags: `run-main WordCount -- scoobi warning.times`

Note that logs can be turned off by using the 'quiet' argument:  `run-main WordCount -- scoobi quiet` (you can also override the `quiet` method to return `true`)


### Arguments

The arguments passed on the command lines are stripped from Hadoop's arguments (used to setup the Hadoop configuration object) and the remaining arguments are accessible with 2 methods:

 - `args`: the user arguments
 - `scoobiArgs`: the Scoobi-specific arguments (verbose, times,...)

Note that all the command line arguments are still accessible with the `commandLineArguments` method.

### Summary

The Scoobi command line arguments must be passed as dot separated values after the `scoobi` argument name:

  `run-main WordCount -- scoobi value1.value2.value3`

 Name           | Default value                    | Description
 ---------------|----------------------------------|--------------------------------------------------------------------
 verbose        | true                             | if defined, log statements are displayed
 quiet          | false                            | if defined, log statements are not displayed
 times          | false                            | if defined the total execution time is logged
 level          | info                             | minimum level of statements to log
 category       | .*                               | regular expression. By default everything is logged. Use `scoobi` to display only Scoobi logs
 local          | false                            | if defined, run the Scoobi job locally
 deletelibjars  | false                            | if defined, remove jars from the `libjars` directory before uploading them again
 keepfiles      | false                            | if defined, temp files and working directory files are not deleted after the job execution (only for testing)

  """
}

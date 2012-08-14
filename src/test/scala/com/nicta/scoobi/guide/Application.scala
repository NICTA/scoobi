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

  * you inherit an implicit `ScoobiConfiguration` object, for persisting `DLists`. The `ScoobiConfiguration` object encapsulates a Hadoop `Configuration` object setup with the Hadoop command-line arguments, and is accessible through the `configuration` variable.

### Arguments

ScoobiApp provides support for passing in Hadoop-configuration and Scoobi-configuration arguments, which are stripped from the command line prior to being made available in the `args` variable.

The format of the command line is as follows:

  `COMMAND-TO-INVOKE-PROGRAM [HADOOP-ARGS ...] [APPLICATION-ARGS ...] [-- scoobi VALUE1.VALUE2.VALUE3]`

where `COMMAND-TO-INVOKE-PROGRAM` is the invocation needed to run the program, `HADOOP-ARGS` are optional arguments to configure Hadoop, `APPLICATION-ARGS` are the arguments to the application itself, and everything else specifies arguments to configure Scoobi.  Note that both the Hadoop-configuration and Scoobi-configuration arguments are optional.

A simple example that runs an application `mypackage.MyApp` using `sbt run-main` with application arguments `input-dir output-dir` and without any configuration-level arguments is:

  `sbt run-main mypackage.MyApp input-dir output-dir`

A more complicated example is as follows:

  `sbt run-main mypackage.MyApp -Dmapred.max.map.failures.percent=20 -Dmapred.max.reduce.failures.percent=20 --by-time input-dir output-dir -- scoobi warn.times`

This command contains the following sets of arguments:

1. The arguments `-Dmapred.max.map.failures.percent=20 -Dmapred.max.reduce.failures.percent=20` are handled by Hadoop. (They allow for 20% of the map or reduce tasks to fail and the job as a whole to still succeed.)
2. The arguments `--by-time input-dir output-dir` are passed to the application itself.
3. The arguments `warn.times` are handled by Scoobi. (They set the log level to WARN and cause logging of execution time.)

The various sets of arguments are accessible from the `ScoobiApp` with the following variables:

1. `args`: The application arguments.
2. `scoobiArgs`: The Scoobi-configuration arguments, split on ".".
3. `commandLineArguments`: The entire set of command-line arguments.

### Scoobi Configuration Arguments

The following are the possible configuration arguments for Scoobi:


 Name           | Default value                    | Description
 ---------------|----------------------------------|--------------------------------------------------------------------
 verbose        | true                             | if defined, log statements are displayed
 quiet          | false                            | if defined, log statements are not displayed
 times          | false                            | if defined the total execution time is logged
 level          | info                             | minimum level of statements to log
 category       | .*                               | regular expression. By default everything is logged. Use `scoobi` to display only Scoobi logs
 local          | false                            | if defined, run the Scoobi job locally
 deletelibjars  | false                            | if defined, remove jars from the `libjars` directory before uploading them again
 nolibjars      | false                            | if defined, do not upload dependent jars to the `libjars` directory and include the Scoobi jar in the job jar
 useconfdir     | false                            | if defined, use the configuration files in `$HADOOP_HOME/conf` (useful when running apps from inside sbt)
 keepfiles      | false                            | if defined, temp files and working directory files are not deleted after the job execution (only for testing)

The list of possible log levels is `all`, `fatal`, `info`, `warn`, `trace`, `off`.

It is also possible to change configuration values by overriding methods in `ScoobiApp`.

### Dependencies

The `ScoobiApp` trait extends the `LibJars` trait which provides functionalities to manage dependent jars. More specifically it helps you load all the dependent jars on the cluster before executing your Scoobi job:

 1. the dependent jars are taken from the context classloader and are only loaded if they come from a `.ivy2` or `.m2` directory (the Hadoop jars are *not* included). This behavior can be redefined by overriding the `jars` method

 2. `uploadLibJarFiles` uploads all the new jars to a directory on the cluster. By default this directory is named `libjars` (this is declared in the `libJarsDirectory` method). When you upload the jars, *all* the dependencies will be added to the `mapred.classpath` Hadoop configuration variable)

 3. `deleteJars` can be used to remove all existing jars in the `libjars` directory if one of your dependencies has changed (but kept the same name)

Here is an example on how to use these methods:

```scala
package mypackage

object MyApp extends ScoobiApp {
  def run() {
    // remove the existing jars on the cluster
    deleteJars
    // upload the dependencies to the cluster
    uploadLibJarFiles

    // define and execute a Scoobi job
    val lines = ...
  }
}
```

If you don't want to use this facility at all, you can override the `upload` method and set it to `false` or pass `scoobi nolibjars` on the command line.

### Configuration

The `ScoobiApp` trait can be used in 2 different contexts:

  1. with the `hadoop jar` command

    In that case, `hadoop` finds the cluster location by either using the configuration files in its `conf` directory or by using the `fs` and `jt` arguments

  2. within sbt

    In that case the cluster location can be either defined by using the `useconfdir` command line argument to get the configuration files found in `$HADOOP_HOME/conf` directory
    You can also override the `fs` and `jobTracker` methods, and get those values from your own properties file.

### Local execution

For testing, it can useful to run Scoobi applications locally only:

 - if you are using the `hadoop jar` command, you can specify a local configuration by either using the `fs`/`jt` arguments
 - if you are using sbt, you can pass the "`scoobi local`" argument for a local execution

### Logging

By default, when extending the `Hadoop` trait, Hadoop and Scoobi logs will be shown in the console at the "INFO" level. However for debugging failed jobs you may want to change the log level or the log categories:

 * show some log levels: `run-main mypackage.MyApp [ARGS] -- scoobi warn` (you can also override the `level` method). The log levels are the ones from the Apache commons logging library: `ALL`, `FATAL`, `INFO`, `WARN`, `TRACE`

 * show some log categories: `run-main mypackage.MyApp [ARGS] -- scoobi warn.(hadoop|scoobi)` will only display the log lines where the category matches `.*(hadoop|scoobi).*`. Note that you can visually separate this regular expression for log categories with brackets to help the reading: `run mypackage.MyApp -- scoobi.warn.[(hadoop|scoobi)].times`

 * you can additionally show the execution times, locally and on the cluster: `run-main mypackage.MyApp -- scoobi times` (or override the `showTimes` method)

 * finally you can combine those flags: `run-main mypackage.MyApp [ARGS] -- scoobi warn.times`

Note that logs can be turned off by using the 'quiet' argument:  `run-main mypackage.MyApp [ARGS] -- scoobi quiet` (you can also override the `quiet` method to return `true`)

  """
}

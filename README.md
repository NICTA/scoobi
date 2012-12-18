
## Welcome!
## ReadMe

[Hadoop MapReduce](http://hadoop.apache.org/) is awesome, but it seems a little bit crazy when you have to write [this](http://wiki.apache.org/hadoop/WordCount) to count words. Wouldn't it be nicer if you could simply write what you want to do:

```scala
val lines = fromTextFile("hdfs://in/...")

val counts = lines.flatMap(_.split(" "))
                .map(word => (word, 1))
                .groupByKey
                .combine(_+_)

persist(toTextFile(counts, "hdfs://out/...", overwrite=true))
```

This is what Scoobi is all about. Scoobi is a Scala library that focuses on making you more productive at building Hadoop applications. It stands on the functional programming shoulders of Scala and allows you to just write **what** you want rather than **how** to do it.

Scoobi is a library that leverages the Scala programming language to provide a programmer friendly abstraction around Hadoop's MapReduce to facilitate rapid development of analytics and machine-learning algorithms.

### Install

See the [install instructions](http://nicta.github.com/scoobi/guide/Quick%20Start.html#Installing+Scoobi) in the QuickStart section of the [User Guide](http://nicta.github.com/scoobi/guide/User%20Guide.html).

### Features

 * Familiar APIs - the `DList` API is very similar to the standard Scala `List` API

 * Strong typing - the APIs are strongly typed so as to catch more errors at compile time, a
 major improvement over standard Hadoop MapReduce where type-based run-time errors often occur

 * Ability to parameterise with rich [data types](http://nicta.github.com/scoobi/guide/Data%20Types.html) - unlike Hadoop MapReduce, which requires that you go off implementing a myriad of classes that implement the `Writable` interface, Scoobi allows `DList` objects to be parameterised by normal Scala types including value types (e.g. `Int`, `String`, `Double`), tuple types (with arbitrary nesting) as well as **case classes**

 * Support for multiple types of I/O - currently built-in support for [text](http://nicta.github.com/scoobi/guide/Input%20and%20Output.html#Text+files), [Sequence](http://nicta.github.com/scoobi/guide/Input%20and%20Output.html#Sequence+files) and [Avro](http://nicta.github.com/scoobi/guide/Input%20and%20Output.html#Avro+files) files with the ability to implement support for [custom sources/sinks](http://nicta.github.com/scoobi/guide/Input%20and%20Output.html#Custom+sources+and+sinks)

 * Optimization across library boundaries - the optimiser and execution engine will assemble Scoobi code spread across multiple software components so you still keep the benefits of modularity

 * It's Scala - being a Scala library, Scoobi applications still have access to those precious Java libraries plus all the functional programming and consise syntax that makes developing Hadoop applications very productive

 * Apache V2 licence - just like the rest of Hadoop

### Getting Started

To get started, read the [getting started steps](http://nicta.github.com/scoobi/guide/Quick%20Start.html) and the section on [distributed lists](http://nicta.github.com/scoobi/guide/Distributed%20Lists.html). The remaining sections in the [User Guide](http://nicta.github.com/scoobi/guide/User%20Guide.html) provide further detail on various aspects of Scoobi's functionality.

The user mailing list is at <http://groups.google.com/group/scoobi-users>. Please use it for questions and comments!

### Community

 * [![Build Status](https://secure.travis-ci.org/NICTA/scoobi.png)](http://travis-ci.org/NICTA/scoobi)
 * [Issues](https://github.com/NICTA/scoobi/issues)
 * [Change history](http://notes.implicit.ly/tagged/scoobi)
 * [Source code (github)](https://github.com/NICTA/scoobi)
 * [API Documentation](http://nicta.github.com/scoobi/api/SCOOBI-0.5.0-cdh4/index.html)
 * [Examples](https://github.com/NICTA/scoobi/tree/SCOOBI-0.5.0-cdh4/examples)
 * User Guide for the [SNAPSHOT](http://nicta.github.com/scoobi/guide-SNAPSHOT/guide/User%20Guide.html) version ([latest api](http://nicta.github.com/scoobi/api/master/scala/index.html))
 * Mailing Lists: [scoobi-users](http://groups.google.com/group/scoobi-users), [scoobi-dev](http://groups.google.com/group/scoobi-dev)
  

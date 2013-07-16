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

import org.specs2.specification.{SpecStart, Text, Fragments}
import org.specs2.Specification
import ScoobiVariables._

/**
 * This class generates the README.md for github http://github.com/NICTA/scoobi
 */
class ReadMe extends Specification { def is =
  "Welcome!".title.urlIs("README.md") ^
  "[![Build Status](https://travis-ci.org/NICTA/scoobi.png?branch=master)](https://travis-ci.org/NICTA/scoobi)"^
  inline(ReadMe)
}

object ReadMe extends ScoobiPage { def is = args.report(notoc=true) ^
  s2"""
[Hadoop MapReduce](http://hadoop.apache.org/) is awesome, but it seems a little bit crazy when you have to write [this](http://wiki.apache.org/hadoop/WordCount) to count words. Wouldn't it be nicer if you could simply write what you want to do:${snippet{
import Scoobi._, Reduction._

val lines = fromTextFile("hdfs://in/...")

val counts = lines.mapFlatten(_.split(" "))
               .map(word => (word, 1))
               .groupByKey
               .combine(Sum.int)

counts.toTextFile("hdfs://out/...", overwrite=true).persist(ScoobiConfiguration())
}}

This is what Scoobi is all about. Scoobi is a Scala library that focuses on making you more productive at building Hadoop applications. It stands on the functional programming shoulders of Scala and allows you to just write **what** you want rather than **how** to do it.

Scoobi is a library that leverages the Scala programming language to provide a programmer friendly abstraction around Hadoop's MapReduce to facilitate rapid development of analytics and machine-learning algorithms.

### Install

See the [install instructions](${GUIDE_OFFICIAL_PAGE}com.nicta.scoobi.guide.QuickStart.html#Installing+Scoobi) in the QuickStart section of the [User Guide](${GUIDE_OFFICIAL_PAGE}com.nicta.scoobi.guide.UserGuide.html).

### Features

 * Familiar APIs - the `DList` API is very similar to the standard Scala `List` API

 * Strong typing - the APIs are strongly typed so as to catch more errors at compile time, a
 major improvement over standard Hadoop MapReduce where type-based run-time errors often occur

 * Ability to parameterise with rich [data types](${GUIDE_OFFICIAL_PAGE}com.nicta.scoobi.guide.DataTypes.html) - unlike Hadoop MapReduce, which requires that you go off implementing a myriad of classes that implement the `Writable` interface, Scoobi allows `DList` objects to be parameterised by normal Scala types including value types (e.g. `Int`, `String`, `Double`), tuple types (with arbitrary nesting) as well as **case classes**

 * Support for multiple types of I/O - currently built-in support for [text](${GUIDE_OFFICIAL_PAGE}com.nicta.scoobi.guide.LoadAndPersist.html#Text+files), [Sequence](${GUIDE_OFFICIAL_PAGE}com.nicta.scoobi.guide.LoadAndPersist.html#Sequence+files) and [Avro](${GUIDE_OFFICIAL_PAGE}com.nicta.scoobi.guide.LoadAndPersist.html#Avro+files) files with the ability to implement support for [custom sources/sinks](${GUIDE_OFFICIAL_PAGE}com.nicta.scoobi.guide.LoadAndPersist.html#Custom+input)

 * Optimization across library boundaries - the optimiser and execution engine will assemble Scoobi code spread across multiple software components so you still keep the benefits of modularity

 * It's Scala - being a Scala library, Scoobi applications still have access to those precious Java libraries plus all the functional programming and concise syntax that makes developing Hadoop applications very productive

 * Apache V2 licence - just like the rest of Hadoop

### Getting Started

To get started, read the [getting started steps](${GUIDE_OFFICIAL_PAGE}com.nicta.scoobi.guide.QuickStart.html) and the section on [distributed lists](${GUIDE_OFFICIAL_PAGE}com.nicta.scoobi.guide.DistributedLists.html). The remaining sections in the [User Guide](${GUIDE_OFFICIAL_PAGE}com.nicta.scoobi.guide.UserGuide.html) provide further detail on various aspects of Scoobi's functionality.

The user mailing list is at <http://groups.google.com/group/scoobi-users>. Please use it for questions and comments!

### Community

 * [Issues](https://github.com/NICTA/scoobi/issues)
 * [Change history](http://notes.implicit.ly/tagged/scoobi)
 * [Source code (github)](https://github.com/NICTA/scoobi)
 * [API Documentation](${API_OFFICIAL_PAGE})
 * [Examples](https://github.com/NICTA/scoobi/tree/${OFFICIAL_TAG}/examples)
 * User Guide for the [SNAPSHOT](${GUIDE_SNAPSHOT_PAGE}com.nicta.scoobi.guide.UserGuide.html) version ([latest api](${API_SNAPSHOT_PAGE}))
 * Mailing Lists: [scoobi-users](http://groups.google.com/group/scoobi-users), [scoobi-dev](http://groups.google.com/group/scoobi-dev)
  """

  override def map(fs: =>Fragments) =
    fs.map {
      case start: SpecStart => start.baseDirIs(".")
      case other            => other
    }

}

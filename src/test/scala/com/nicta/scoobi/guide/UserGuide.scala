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

import org.specs2.specification.{Fragments, SpecificationStructure}
import org.specs2.html.MarkdownLink
import Scoobi._

class UserGuide extends ScoobiPage { def is = args.report(notoc=false) ^ "User Guide".title^ s2"""

<notoc><h4>Scoobi - Bringing the productivity of Scala to Hadoop</h4></notoc>

[Hadoop MapReduce](http://hadoop.apache.org) is awesome, but it seems a little bit crazy when you have to write [this](http://wiki.apache.org/hadoop/WordCount) to count words. Wouldn't it be nicer if you could simply write what you want to do: ${snippet{

val lines = fromTextFile("hdfs://in/...")

val counts = lines.mapFlatten(_.split(" ")).
                   map(word => (word, 1)).
                   groupByKey.
                   combine(Reduction.Sum.int)

counts.toTextFile("hdfs://out/...").persist
}}

This is what Scoobi is all about. Scoobi is a Scala library that focuses on making you more productive at building Hadoop applications. It stands on the functional programming shoulders of Scala and allows you to just write **what** you want rather than **how** to do it.

Scoobi is a library that leverages the Scala programming language to provide a programmer friendly abstraction around Hadoop's MapReduce to facilitate rapid development of analytics and machine-learning algorithms.

In this user guide, you will find:

 * ${ ("a"                                 ~ (qs, "guide")).linkMarkdown }
 * ${ ("an overview of Scoobi's DLists:"   ~ dl).linkMarkdown } ${ ("and" ~ dobj).linkMarkdown }
 * ${ ("how to"                            ~ load).linkMarkdown }
 * ${ ("how to support your own"           ~ data).linkMarkdown }
 * ${ ("how to use"                        ~ gp).linkMarkdown }
 * ${ (""                                  ~ ("extensions", ext, "for db-like programming")).linkMarkdown }
 * ${ ("how to create"                     ~ ("Scoobi applications", app)).linkMarkdown }
 * ${ ("a"                                 ~ ("testing guide", ts)).linkMarkdown }
 * ${ (""                                  ~ (dply, "instructions")).linkMarkdown }
 * ${ ("some"                              ~ adv).linkMarkdown }
 * ${ ("how to get involved in"            ~ dev).linkMarkdown }
 """ ^
 link(qs    .hide)^
 link(app   .hide)^
 link(dl    .hide)^
 link(dobj  .hide)^
 link(load  .hide)^
 link(data  .hide)^
 link(gp    .hide)^
 link(ext   .hide)^
 link(ts    .hide)^
 link(dply  .hide)^
 link(adv   .hide)^
 link(dev   .hide)

  val qs    = new QuickStart
  val app   = new Application
  val dl    = new DistributedLists
  val dobj  = new DistributedObjects
  val data  = new DataTypes
  val gp    = new Grouping
  val load  = new LoadAndPersist
  val ext   = new Extensions
  val ts    = new Testing
  val dply  = new Deployment
  val adv   = new Advanced
  val dev   = new ScoobiDevelopment

  implicit lazy val configuration: ScoobiConfiguration = ScoobiConfiguration()
}

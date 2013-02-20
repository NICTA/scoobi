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

import org.specs2.specification.SpecificationStructure
import org.specs2.html.MarkdownLink

class UserGuide extends ScoobiPage { def is = args.report(notoc=false) ^"User Guide".title^
                                                                                                                        """
<notoc><h4>Scoobi - Bringing the productivity of Scala to Hadoop</h4></notoc>

[Hadoop MapReduce](http://hadoop.apache.org/) is awesome, but it seems a little bit crazy when you have to write [this](http://wiki.apache.org/hadoop/WordCount) to count words. Wouldn't it be nicer if you could simply write what you want to do:

    val lines = fromTextFile("hdfs://in/...")

    val counts = lines.flatMap(_.split(" "))
                      .map(word => (word, 1))
                      .groupByKey
                      .combine(_+_)

    persist(toTextFile(counts, "hdfs://out/..."))

This is what Scoobi is all about. Scoobi is a Scala library that focuses on making you more productive at building Hadoop applications. It stands on the functional programming shoulders of Scala and allows you to just write **what** you want rather than **how** to do it.

Scoobi is a library that leverages the Scala programming language to provide a programmer friendly abstraction around Hadoop's MapReduce to facilitate rapid development of analytics and machine-learning algorithms.

In this user guide, you will find:                                                                                      """^
                                                                                                                        p^
  Seq(
   link("a"                                , qs, "guide"                                     ),
   link("an overview of Scoobi's concepts:", dl, link(",", dobj)                             ),
   link("the supported"                    , inout - "Input and Output types"                ),
   link("how to support your own"          , data                                            ),
   link("how to use"                       , gp                                              ),
   link("how to"                           , load                                            ),
   link(                                     ext - "extensions", "for db-like programming"   ),
   link("how to create"                    , app - "Scoobi applications"                     ),
   link("a"                                , ts - "testing guide"                            ),
   link(                                     dply, "instructions"                            ),
   link("some"                             , adv                                             ),
   link("get involved in"                  , dev                                             )
    ).map("* "+_).mkString("\n")                                                                                        ^
                                                                                                                        p^
   link(all.map(_.hide))                                                                                                ^
                                                                                                                        end

  def link(pre: String, l1: MarkdownLink): String =
    pre+" "+l1

  def link(l1: MarkdownLink, post: String): String =
    l1+" "+post

  def link(pre: String, spec: SpecificationStructure, l1: MarkdownLink): String =
     link(pre, spec, l1.toString)

  def link(spec: SpecificationStructure, post: String): String =
    spec.markdownLink.fromTop+" "+post

  def link(pre: String, spec: SpecificationStructure): String =
    pre+" "+spec.markdownLink.fromTop

  def link(pre: String, spec: SpecificationStructure, post: String): String =
    pre+" "+spec.markdownLink.fromTop+" "+post

  implicit def toTopLink(s: ScoobiPage): TopLink = new TopLink(s)
  class TopLink(s: ScoobiPage) {
    def - (linkName: String) = s.markdownLink(linkName).fromTop
  }

  lazy val all = Seq(qs, dl, dobj, inout, data, gp, load, ext, app, ts, dply, adv, dev)

  val qs    = new QuickStart
  val app   = new Application
  val dl    = new DistributedLists
  val dobj  = new DistributedObjects
  val data  = new DataTypes
  val inout = new InputOutput
  val gp    = new Grouping
  val load  = new LoadAndPersist
  val ext   = new Extensions
  val ts    = new Testing
  val dply  = new Deployment
  val adv   = new Advanced
  val dev   = new ScoobiDevelopment

}

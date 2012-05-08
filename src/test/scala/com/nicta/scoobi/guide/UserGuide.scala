package com.nicta.scoobi.guide

class UserGuide extends ScoobiPage { def is = args.report(notoc=false) ^"User Guide".title                                                        ^
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
   "a " + qs.markdownLink.up + " guide",
   "an overview of Scoobi's concepts: " + dl.markdownLink.up + ", " + dobj.markdownLink.up,
   "the supported " + inout.markdownLink("Input and Output types").up,
   "how to support your own " + data.markdownLink.up,
   "how to use " + gp.markdownLink.up,
   "extensions ".markdownLink(ext).up + "for db-like programming",
   "a "+ ts.markdownLink("testing guide").up,
   "how to " + bpd.markdownLink("build, package and deploy").up).map("* "+_).mkString("\n")                              ^
                                                                                                                        p^
   link(all.map(_.hide))                                                                                                ^
                                                                                                                        end

  lazy val all = Seq(qs, dl, dobj, inout, data, gp, ext, bpd, ts)

  val qs    = new QuickStart
  val dl    = new DistributedLists
  val dobj  = new DistributedObjects
  val data  = new DataTypes
  val inout = new InputOutput
  val gp    = new Grouping
  val ext   = new Extensions
  val bpd   = new BuildingPackagingDeploying
  val ts    = new Testing

}
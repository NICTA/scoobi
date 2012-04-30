package com.nicta.scoobi.guide

import org.specs2.Specification

class UserGuide extends Specification { def is = "User Guide".title                                                     ^
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
    "a " ~ ("quick start guide", new QuickStart)                                                                        ^
    "an " ~ ("overview of Scoobi's concepts", new Overview)                                                             ^
    "various ways to " ~ ("load and persist data", new LoadAndPersist)                                                  ^
    "support your own " ~ ("data types", new DataTypes)                                                                 ^
    "a " ~ ("testing guide", new Testing)                                                                               ^
                                                                                                                        end

}
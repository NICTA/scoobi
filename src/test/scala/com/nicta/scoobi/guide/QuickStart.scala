package com.nicta.scoobi.guide

class QuickStart extends ScoobiPage { def is = "Quick Start".title^
                                                                                                                        """
### Installing Scoobi

Scoobi has the following requirements:

* [Cloudera's Hadoop 0.20.2](http://www.cloudera.com/hadoop/)
* [Scala 2.9.1](http://www.scala-lang.org/downloads): Note that this is typically set in build.sbt
* [Sbt 0.11.0](https://github.com/harrah/xsbt/wiki)

Scala and Hadoop are obvious prerequisites. In addition, the Scoobi library and Scoobi applications use [sbt](https://github.com/harrah/xsbt/wiki) (version 0.11 or later) for dependency management and building.

**NOTE**: You will probably have to edit the `sbt` launcher script (located in `~/bin/sbt` or wherever `sbt` has been installed) to increase the maximum heap size, or you will get out-of-memory errors.  Try changing the existing `-Xmx` option to `-Xmx2048M` (or adding this option if it's not already present).  If this still leads to errors, `-Xmx4096M` should be enough.


### Building Word Count

To build Scoobi:

      $ cd scoobi
      $ sbt publish-local

Then build and package one of the examples:

      $ cd examples/wordCount
      $ sbt package-hadoop

### Deploying to Hadoop

Finally, run on Hadoop:

      $ hadoop jar ./target/Scoobi_Word_Count-hadoop-0.1.jar <input> <output>
                                                                                                                        """
}

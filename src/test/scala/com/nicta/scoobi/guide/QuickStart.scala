package com.nicta.scoobi.guide

class QuickStart extends ScoobiPage { def is = "Quick Start".title^
                                                                                                                        """
### Installing Scoobi

Scoobi has the following requirements:

* [Cloudera's Hadoop 0.20.2 (CDH3)](http://www.cloudera.com/hadoop/)
* [Sbt 0.11.3](https://github.com/harrah/xsbt/wiki)
* [Scala 2.9.2](http://www.scala-lang.org/downloads): Note that this is typically set, and fetched with sbt

Scala and Hadoop are obvious prerequisites. In addition, the Scoobi library and Scoobi applications use [sbt](https://github.com/harrah/xsbt/wiki) (version 0.11.3) for dependency management and building.

**NOTE**: You will probably have to edit the `sbt` launcher script (located in `~/bin/sbt` or wherever `sbt` has been installed) to increase the maximum heap size, or you will get out-of-memory errors.  Try changing the existing `-Xmx` option to `-Xmx2048M` (or adding this option if it's not already present).  If this still leads to errors, `-Xmx4096M` should be enough.

End users typically should avoid manually building scoobi, and use the prebuilt jars on sonatype.

### Building Word Count

To build a scoobi application (e.g. the included wordCount example):

      $ cd examples/wordCount
      $ sbt package-hadoop

### Deploying to Hadoop

Finally, run on Hadoop:

      $ hadoop jar ./target/Scoobi_Word_Count-hadoop-0.1.jar <input> <output>

Note that there appears to be an OSX-specific [issue](https://github.com/NICTA/scoobi/issues/1) associated with calling hadoop in this manner requiring the jar to be added to HADOOP_CLASSPATH and then hadoop being given the correct object to run. e.g.:

      $ export HADOOP_CLASSPATH=$PWD/target/Scoobi_Word_Count-hadoop-0.1.jar
      $ hadoop WordCount inputFile/to/wordcount nonexistent/outputdir                                                                                                                """
}

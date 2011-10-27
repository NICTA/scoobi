Scoobi - Scala for Hadoop
=========================

Scoobi is a library that leverages the Scala programming language to provide a
programmer friendly abstraction around Hadoop's MapReduce to facilitate rapid
development of analytics and machine-learning algorithms.


Technical Details
----------------

Scoobi is centered around a type-safe distributed collection [DList](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.DList)
objects that abstract data in HDFS. The user can manipulate these distributed collections
with parallel operations on user defined functions. When the final result is required
(e.g. [persist](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.DList) to a file), Scoobi will
optimize these logical operations into an efficient MapReduce pipeline to send to the hadoop cluster.

Because of this, writing Scoobi is more expressive, quicker and easier to code, and often
more efficient than writing manual MapReduce jobs.

Quick Start
----------

* [Install Scala](http://www.scala-lang.org/downloads) (This is only a good idea, but it can be skipped if you only use sbt to build and run)
* [Install Hadoop](http://www.cloudera.com/hadoop/)
* [Install Sbt](https://github.com/harrah/xsbt/wiki/Getting-Started-Setup) Version 0.11.0 or greater. Note: When creating the sbt launcher script, it is worth increasing the max heapspace (the number given to -Xmx) to prevent sbt running out of memory when building.

Download Scoobi:

        $ git clone https://github.com/nicta/scoobi.git

Change into the directory

        $ cd scoobi

Locally deploy scoobi

        $ sbt publish-local


Illustrative example
-------------------

Note: there's a top level folder [examples](scoobi/tree/master/examples/) which contain a number of self-contained
tutorial-like examples, as well as a [guide](scoobi/blob/master/examples/README.md) to building and deploying them.
This makes a better starting point for learning and using scoobi, however, for illustrative purposes
below is a how a word count program could be written in Scoobi:

        import com.nicta.scoobi._

        object WordCount {
          def main(args: Array[String]) {

            val lines: DList[String] = TextInput.fromTextFile(args(0))

            val fq: DList[(String, Int)] = lines.flatMap(_.split(" "))
                                                .map { w => (w, 1) }
                                                .groupByKey
                                                .combine((_+_))

            DList.persist(TextOutput.toTextFile(fq, args(1)))
          }
        }

This program will take two arguments, an input file (to word count) and a non-existent directory to create for the output.
For more information, see the [examples](scoobi/tree/master/examples/) directory and its corresponding README.md

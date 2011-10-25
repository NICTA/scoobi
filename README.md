Scoobi - Scala for Hadoop
=========================

Scoobi is a library that leverages the Scala's language to provide a
programmer friendly abstraction of the Hadoop Map-Reduce programming
model to facilitate the rapid development of analytics and machine-learning.
algorithms that target Hadoop by providing a distributed
collections abstraction:

* Distributed collection objects abstract data in HDFS
* Methods on these objects abstract map/reduce operations
* Programs manipulate distributed collection objects
* Scoobi turns these manipulations into MapReduce jobs
* Based on Google's FlumeJava

Technical Details
--------

Scoobi is centered around a type-safe distributed collection [DList](http://scoobi dlist api)
objects that abstract data in HDFS. The user can manipulate these distributed collections
with parallel operations and user defined Scala and Java functions. When the final result
is required (e.g. [persist]() to a file), Scoobi will optimize these logical operations
into an efficient MapReduce pipeline and execute it.

Because of this, writing Scoobi is more expressive, quicker to code, easier to read and
often more efficient than writing manual MapReduce jobs.


Quick Start
----------


* [Install Scala](http://www.scala-lang.org/downloads) (This is only a good idea, but this step can technically be skipped as sbt will download a version for itself)
* [Install Hadoop](http://www.cloudera.com/hadoop/)
* [Install Sbt](https://github.com/harrah/xsbt/wiki/Setup) version 0.11.0 (Note: When creating the sbt launcher script, it is probably worth increasing the max heapspace (the number given to -Xmx) to prevent sbt running out of memory when building)


Download Scoobi:

        $ git clone https://github.com/nicta/scoobi.git

Change into the directory

        $ cd scoobi

Locally deploy scoobi

        $ sbt publish-local


Illustrative example
-------------------

Note: there's a top level folder [examples](examples/) in scoobi which contain a number of
self-contained tutorial-like examples, and a guide to building and deploying them. These make
a better starting point for learning and using scoobi. However, for illustrative purposes,
below is how a word count program could be written in Scoobi:

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

This program will take two arguments, an input file (to word count) and a non-existent directory to create for the output. For more information, see [examples](examples/) directory and its corresponding README

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


Creating Your own Scoobi Project
-------------------

Note: there's a top level folder [examples](https://github.com/NICTA/scoobi/tree/master/examples) which contain a number of self-contained
tutorial-like examples, as well as a [guide](https://github.com/NICTA/scoobi/blob/master/examples/README.md) to building and deploying them.
This makes a better starting point for learning and using scoobi, however, for illustrative purposes below is how
you would go about creating a Soobi based program from scratch:

Creating and changing into a new project directory. e.g.

        $ mkdir wordCount
        $ cd wordCount

We will use [sbt](https://github.com/harrah/xsbt/wiki) to build and deploy our project. Scoobi has a very handy [sbt plugin](https://github.com/NICTA/sbt-scoobi)
which can greatly simplify the process of deploying, by creating a self-contained executable jar that can easily be given to hadoop.

So we will the directory structure expected by sbt:

        $ mkdir project
        $ mkdir -p src/main/scala

To use the sbt-scoobi plugin, we simply create a file at `project/plugins.sbt` with the following contents:

        resolvers += "Scoobi deploy" at "http://nicta.github.com/sbt-scoobi/repository/"
        
        addSbtPlugin("com.nicta" %% "sbt-scoobi" % "0.0.1")

And now we can create a pretty standard `build.sbt` e.g.

        name := "Scoobi Word Count"
        
        version := "0.1"
        
        scalaVersion := "2.9.1"
        
        libraryDependencies += "com.nicta" %% "scoobi" % "0.0.1" % "provided"

The "provided" is added to the scoobi dependency to let sbt know that scoobi
is provided by the sbt-plugin when it packages everything in a jar. If you
don't included this "provided" nothing bad will happen, but the jar will contain
some scoobi dependencies that are not strictly required.

For more information on sbt-scoobi and some extra options, see its [page](https://github.com/NICTA/sbt-scoobi)

And finally, we create our source code at `src/main/scala/WordCount.scala`:

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

Using sbt, we can easily build and package this to a file ready to give to hadoop:

        $ sbt package-hadoop

The location of our newly contained jar will be outputted, but by default would be" `target/Scoobi_Word_Count-hadoop-0.1.jar`

We can now run our program with:

        $ hadoop jar target/Scoobi_Word_Count-hadoop-0.1.jar inputFile/to/wordcount nonexistent/outputdir

Note: There appears to be a mac-specific issue, where the jar must be added to HADOOP_CLASSPATH and then hadoop given the
correct object to run. e.g.:

        export HADOOP_CLASSPATH=$PWD/target/Scoobi_Word_Count-hadoop-0.1.jar
        hadoop WordCount inputFile/to/wordcount nonexistent/outputdir

This [issue](https://github.com/NICTA/scoobi/issues/1) is currently under investigation

Questions? Problems? Bugs? Help?
--------------------------------

Please use our [https://github.com/NICTA/scoobi/issues](issue tracker) for any bugs, issues, problems or questions you might have. Please tag "questions" accordingly

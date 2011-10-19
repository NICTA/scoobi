Scoobi - Scala for Hadoop
=========================

Scoobi is a library that leverages Scala's language extensibility features
to provide a programmer friendly abstraction of the Hadoop Map-Reduce
programming model to address this need. Scoobi facilitates:

1. Rapid development of analytics and machine-learning algorithms that target Hadoop;
2. Implementation of domain specific languages on top of it (e.g. a DSL for graph
processing or a DSL for linear algebra) known as Scoobi Snaxs.


Quick Start
----------


* [Install Scala](http://www.scala-lang.org/downloads)
* [Install Hadoop](http://www.cloudera.com/hadoop/)
* [Install Sbt](https://github.com/harrah/xsbt/wiki/Setup) (Note: When creating the sbt launcher script, it is probably worth increasing the max heapspace (the number given to -Xmx) so you don't run out of heapspace when building)


Get a copy of Scoobi:
* git clone ...

Change into the directory
* $ cd scoobi

Launch sbt
* $ sbt

Build Scoobi, and locally deploy it
* > publish-local

Now we can exit sbt
* > exit


Sample Project
--------------

Now that we have Scoobi installed. Let's now create a sample scoobi project.

Create a new directory (where ever you like) for the project, and change into it. e.g.

* $ mkdir scoobi_wordcount
* $ cd scoobi_wordcount


Now we must create a configuration file so we can use sbt to build our project. Create a 'build.sbt' with the following contents

    name := "Sample Scoobi Word Count"
    version := "0.1"
    libraryDependencies += "default" %% "scoobi" % "0.0.1"


And now create the following directories for our source

* $ mkdir -p src/main/scala

And create a new file at src/main/scala/word_count.scala with the following contents:
    import com.nicta.scoobi._

    object SampleScoobi {
      def main(args: Array[String]) {

        val lines: DList[String] = TextInput.fromTextFile(args(0))

        val fq: DList[(String, Int)] = lines.flatMap(_.split(" "))
                                            .map { w => (w, 1) }
                                            .groupByKey
                                            .combine((_+_))

        DList.persist(TextOutput.toTextFile(fq, args(1)))
      }
    }


Now back to sbt
$ sbt

we can run our sample project

> run $PATH_OF_TEXT_FILE_TO_WORD_COUNT $PATH_OF_NON_EXISTENT_DIRECTORY_TO_CREATE_WITH_OUTPUT

e.g.

> run src/main/scala/word_count.scala wc-out

Which will word count, the word count source file. And put the results in a new directory 'wc-out'. (Note: If you run this again, you must delete the old 'wc-out' directory, or pick a different name)

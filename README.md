Scoobi - Scala for Hadoop (and hopefully more)
==============================================

Scoobi is a project that is just beginning but we hope it lives up to our
expectations:

The Hope
--------

Pig and Hive have provide significant programmer productivity gains for
implementing a wide range of algorithms on the Hadoop Map-Reduce
programming model. However, these productivity languages do not have good
support for numerically intensive iterative algorithms.

Scoobi is a library that leverages Scala's language extensibility features
to provide a programmer friendly abstraction of the Hadoop Map-Reduce
programming model to address this need. Scoobi facilitates:

1. Rapid development of analytics and machine-learning algorithms that target Hadoop;
2. Implementation of domain specific languages on top of it (e.g. a DSL for graph
processing or a DSL for linear algebra) known as Scoobi Snaxs.

Building
--------

No Maven or Ant scripts yet:

    scalac -d build -cp $HADOOP_HOME/hadoop-core.jar:$HADOOP_HOME/lib/commons-logging.jar src/com/nicta/scoopi/*.scala

Running
-------

To run the canonical *word count* example:

    scala -cp `hadoop classpath`:build com.nicta.scoobi.Test input_files output

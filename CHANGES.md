master relase notes (will become 0.4.0)
--------------

### New Features

* ScoobiApp trait for making scoobi apps even easier

* Single import `import com.nicta.scoobi.Scoobi._`

* IO support for avro

* Sequence files automatically convert to/from writable

* DList reduction methods (e.g. DList.reduce, product, min, etc.)

### Changes

* All methods of Scoobi object are now in Conf object

* `DList.use(x: DObject)` has been removed, use `DObject.use` instead

* To avoid hiding the scala types, the TextInput pattern matcher helpers have been renamed: Int to AnInt, Float to AFloat, Double to ADouble, Long to ALong.

### Improvements

* Lots of bug fixes

* Improvements in optimizer

* MSCR now imposes less overhead on objects that need no fusion

* Join/coGroup API cleaned up, and performance improved




0.3.0 release notes
-------------------
[All Issues fixed](https://github.com/NICTA/scoobi/issues?milestone=1&state=closed)

### New Features

* Support for Sequence files as well as better support in general for adding other I/O types. Essentially, if you have an InputFormat or OutputFormat, you can plug them into a DataSource or DataSink, respectively, and you're good to go. Take a look at how I/O for text files and sequence files are implemented to get the idea.

* The ability to create a DList given a sequence of elements. There is now an `apply` method for DLists and take a look at the `tabulate` method. Also added is pimping of regular collections into DLists using `toDList`. See [DList docs](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.DList)

* A method to write out delimited text files - [TextOutput](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.io.text.TextOutput$).`toDelimitedTextFile`. Thanks to [Alex Cozi](https://github.com/xelax) for this one.

* Better logging. Scoobi now logs input and output paths, how many map-reduce steps will be executed, which step is being executed, map/reduce task progress, number of reducers for the current step, map-
reduce job ids, etc. Turning off standard Hadoop logging in log4j.properties is useful.

* A new way to "persist". There's a new class, 'Job', which allows you to build up a list of the DLists and DObjects (more on those below) you want included as part of the computation. Here's how you use it:

```scala
  val job = Job()
  job << toTextFile(a, "hdfs://...")
  job << toSequenceFile(b, "hdfs://...")
  job.run()
```

This doesn't replace  `persist` but allows you to effectivly "sprinkle" the persisting throughout your code. Note that the call to `run()` triggers the map-reduce job(s) and on completion will clear out the list of DLists that were persisted. That means you can reuse a Job object.

* A new [DList](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.DList) method `materialize`, that turns a `DList[A]` into a `DObject[Iterable[A]]`. A [DObject](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.DObject) is really just a wrapper (there are other plans for it in the future) and you get to the value inside using its `get` method. `materialize` makes it possible to implement iterative algorithms that converge.

Here's how you use it:

```scala
  val a: DList[Int] = ...
  val x: DObject[Iterable[Int]] = a.materialize

  val job = Job()
  job << use(x)  // note: in later versions of scoobi, this becomes 'job << x.use'
  job.run()

  x.get foreach { println }
```

* A bunch of performance improvements relating mostly to reducers and partitioners.

* A `distinct` method on DLists.

* Replacing the `Ordering` type class constraint on 'groupByKey' with a new type class constraint, `Grouping`. `Grouping` is more detailed and is a complete abstraction of all parts of Hadoop's sort-and-shuffle phase: partitioning, value grouping, and key sorting. Implicit values are provided for types with 'Ordering' and types that are 'Comparable'. You can write your own for implementing functionality such as secondary-sorting.

* Simple left and right outer joins for two DLists, with an [illustrative example](https://github.com/nicta/scoobi/blob/master/examples/joinExamples/src/main/scala/JoinExamples.scala)

* Support glob patterns in input path specification.

### Bug Fixes

* Handle MSCRs with a flatten node, but no group by key
* Fix bug for when outputs are added to CopyTable.
* Make finding related nodes less greedy
* Fix bug in generation of identity reducers
* JarBuilder modified to enable running from eclipse
* Fix an issue where Scoobi would die on writing long strings

0.2.0 release notes
-------------------

### New features

* [Java bindings](http://nicta.github.com/scoobi/java/master/index.html)
* Relational functionality: [`join`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.lib.Join$), [`joinOn`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.lib.Join$), [`coGroup`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.lib.CoGroup$) and [`coGroupOn`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.lib.CoGroup$)
* New [`DList`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.DList) methods:
  * `parallelDo` - a sledge-hammer for when `flatMap` isn't enough
  * `collect` - applies a partial function to each element
  * `by` - create a keyed `DList` for join and co-group operations
* [`WireFormat`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.WireFormat) helpers support for algebraic data types (ADTs):
  * `mkAbstractWireFormat`
  * `mkCaseWireFormat` (formerly just `mkWireFormat`)
  * `mkObjectWireFormat`
  Read more on the [wiki page for serialization](https://github.com/NICTA/scoobi/wiki/Serialization)
* Migration to new Hadoop API (i.e. use of *context* objects)
* [*Shortest path* example](https://github.com/NICTA/scoobi/blob/master/examples/shortestPath/src/main/scala/Graph.scala)

### Bug fixes

* Checking all inputs exist before running a job
* Bug fixes in *word count* example
* Typos in documentation
* Speed ups and support for all dataypes in ClassBuilder
* Fix for reference counting intermediate data

Scoobi
------

[Hadoop MapReduce](http://hadoop.apache.org/) is awesome, but it seems a little bit crazy when you have to write [this](http://wiki.apache.org/hadoop/WordCount) to count words. Wouldn't it be nicer if you could simply write what you want to do:

```scala
object WordCount extends ScoobiApp {
  val lines = fromTextFile(args(0))

  val counts = lines.flatMap(_.split(" "))
                    .map(word => (word, 1))
                    .groupByKey
                    .combine(_+_)

  persist(toTextFile(counts, args(1)))
}
```

This is what Scoobi is all about. Scoobi is a Scala library that focuses on making you more productive at building Hadoop applications. It stands on the functional programming shoulders of Scala and allows you to just write **what** you want rather than **how** to do it.

Scoobi is a library that leverages the Scala programming language to provide a programmer friendly abstraction around Hadoop's MapReduce to facilitate rapid development of analytics and machine-learning algorithms.

### Features

 * Familiar APIs - the `DList` API is very similar to the standard Scala `List` API

 * Strong typing - the APIs are strongly typed so as to catch more errors at compile time, a
 major improvement over standard Hadoop MapReduce where type-based run-time errors often occur

 * Ability to parameterize with rich [data types](http://nicta.github.com/scoobi/guide-SNAPSHOT/guide/Data%20Types.html) - unlike Hadoop MapReduce, which requires that you go off implementing a myriad of classes that implement the `Writable` interface, Scoobi allows `DList` objects to be parameterized by normal Scala types including value types (e.g. `Int`, `String`, `Double`), tuple types (with arbitrary nesting) as well as **case classes**

 * Support for multiple types of I/O - currently built-in support for [text](http://nicta.github.com/scoobi/guide-SNAPSHOT/guide/Input%20and%20Output.html#Text+files), [Sequence](http://nicta.github.com/scoobi/guide-SNAPSHOT/guide/Input%20and%20Output.html#Sequence+files) and [Avro](http://nicta.github.com/scoobi/guide-SNAPSHOT/guide/Input%20and%20Output.html#Avro+files) files with the ability to implement support for [custom sources/sinks](http://nicta.github.com/scoobi/guide-SNAPSHOT/guide/Input%20and%20Output.html#Custom+sources+and+sinks)

 * Optimization across library boundaries - the optimizer and execution engine will assemble Scoobi code spread across multiple software components so you still keep the benefits of modularity

 * It's Scala - being a Scala library, Scoobi applications still have access to those precious Java libraries plus all the functional programming and consise syntax that makes developing Hadoop applications very productive

 * Apache V2 licence - just like the rest of Hadoop

### Getting Started

To get started, read the [getting started steps](http://nicta.github.com/scoobi/guide-SNAPSHOT/guide/Quick%20Start.html) and the section on [distributed lists](http://nicta.github.com/scoobi/guide-SNAPSHOT/guide/Distributed%20Lists.html). The remaining sections in the [User Guide](http://nicta.github.com/scoobi/guide-SNAPSHOT/guide/User%20Guide.html) provide further detail on various aspects of Scoobi's functionality.

The user mailing list is at <http://groups.google.com/group/scoobi-users>. Please use it for questions and comments!


### Issues, questions and contributions

Scoobi is released under the Apache license v2. We welcome contributions of bug fixes and/or new
features via GitHib pull requests. In addition, it is important to us to build a friendly user
and developer community around Scoobi, so:

* If you happen to encounter something that looks suspiciously like a bug, be sure to log it on the
[GitHub issue tracker](https://github.com/NICTA/scoobi/issues) so that it can be fixed for everyone - the
more information the better;
* If, on the other hand, you simply have questions about how to use Scoobi, take a look at the posts on the
[scoobi-users](http://groups.google.com/group/scoobi-users) mailing list or post a question of your own;
* And, if you're keen to get your hands dirty and contribute new features to Scoobi, or are hoping to get some
insight into Scoobi's internal architecture, or simply want to know what's going on in developer-land, head
over to the [scoobi-dev](http://groups.google.com/group/scoobi-dev) mailing list.

We will try our best to respond to all issues and questions quickly.


### Links

[User Guide](http://nicta.github.com/scoobi/guide-SNAPSHOT/guide/User%20Guide.html)

[API Documentation](http://nicta.github.com/scoobi/api/master/index.html)

[Examples](https://github.com/NICTA/scoobi/tree/master/examples)

[Change history](https://github.com/NICTA/scoobi/blob/master/CHANGES.md)


### Next Milestone
[0.4.0 Open issues](https://github.com/NICTA/scoobi/issues?milestone=2)

[0.4.0 Closed issues](https://github.com/NICTA/scoobi/issues?milestone=2&state=closed)

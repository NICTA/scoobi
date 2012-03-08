Scoobi - Bringing the productivity of Scala to Hadoop
=====================================================

[Hadoop MapReduce](http://hadoop.apache.org/) is awesome, but it seems a
little bit crazy when you have to write [this](http://wiki.apache.org/hadoop/WordCount)
to count words. Wouldn't it be nicer if you could simply write what you want
to do:

```scala
  val lines = fromTextFile("hdfs://in/...")

  val counts = lines.flatMap(_.split(" "))
                    .map(word => (word, 1))
                    .groupByKey
                    .combine(_+_)

  persist(toTextFile(counts, "hdfs://out/..."))
```

This is what Scoobi is all about. Scoobi is a Scala library that focuses
on making you more productive at building Hadoop applications. It stands on
the functional programming shoulders of Scala and allows you to just write
**what** you want rather than **how** to do it.


Scoobi is a library that leverages the Scala programming language to provide a
programmer friendly abstraction around Hadoop's MapReduce to facilitate rapid
development of analytics and machine-learning algorithms.


Quick start
-----------

Scoobi has the following requirements:

* [Cloudera's Hadoop 0.20.2](http://www.cloudera.com/hadoop/)
* [Scala 2.9.1](http://www.scala-lang.org/downloads): Note that this is typically set in build.sbt
* [Sbt 0.11.0](https://github.com/harrah/xsbt/wiki)

Scala and Hadoop are obvious prerequisites. In addition, the
Scoobi library and Scoobi applications use [sbt](https://github.com/harrah/xsbt/wiki)
(version 0.11 or later) for dependency management and building.

**NOTE**: You will probably have to edit the `sbt` launcher script (located in `~/bin/sbt`
or wherever `sbt` has been installed) to increase the maximum heap size, or you will
get out-of-memory errors.  Try changing the existing `-Xmx` option to `-Xmx2048M`
(or adding this option if it's not already present).  If this still leads to errors,
`-Xmx4096M` should be enough.

To build Scoobi:

```
  $ cd scoobi
  $ sbt publish-local
```

Then build and package one of the examples:

```
  $ cd examples/wordCount
  $ sbt package-hadoop
```

Finally, run on Hadoop:

```
  $ hadoop jar ./target/Scoobi_Word_Count-hadoop-0.1.jar <input> <output>
```


Overview
--------

Scoobi is centered around the idea of a **distributed collection**, which is implemented by the
[`DList`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.DList) (*distributed list*)
class.  In a lot of ways, `DList` objects are similar to normal
[Scala `List`](http://www.scala-lang.org/api/current/scala/collection/immutable/List.html) objects:
they are parameterized by a type and they provide methods that can be used to produce
new `DList` objects, often parameterized by higher-order functions. For example:

```scala
  // Converting a List[Int] to a List[String] keeping only evens
  val stringList = intList filter { _ % 2 == 0 } map { _.toString }

  // Converting a DList[Int] to a DList[String] keeping only evens
  val stringDList = intDList filter { _ % 2 == 0 } map { _.toString }
```

However, unlike a Scala `List` object, the contents of `DList` objects are not stored on
the JVM heap but stored in HDFS. Secondly, calling `DList` methods will not immediately result in
data being generated in HDFS. This is because, behind the scenes, Scoobi implements a
*staging compiler*. The purpose of `DList` methods are to construct a *graph* of data
transformations. Then, the act of *persisting* a `DList` triggers the compilation of the graph
into one or more MapReduce jobs and their execution.


So, `DList` objects essentially provide two abstractions:

1. The contents of a `DList` object abstracts the storage of data and files in HDFS;
2. Calling methods on `DList` objects to transform and manipulate them abstracts the
*mapper*, *combiner*, *reducer* and *sort-and-shuffle* phases of MapReduce.


So, what are some of the advantages of using Scoobi?

* **The collections abstraction implemented by `DList` is a familiar one**: The methods for the
`DList` class have been designed to be the same or as similar to those implemented in the
standard Scala collections. There aren't as many methods, but if you grok the semantics of Scala
collections, you shouldn't have too much trouble getting up to speed with Scoobi;
* **The `DList` class is strongly typed**: Like the Scala collections, the `DList` interface is strongly
typed so that more errors are caught at compile time. This is a major improvement over
standard Hadoop MapReduce where type-based run-time errors often occur;
* **The `DList` class can be easily parameterized on rich data types**: Unlike Hadoop MapReduce,
which requires that you go off implementing a myriad of classes that implement the
`Writable` interface, Scoobi allows `DList` objects to be parameterized by normal Scala types.
This includes the primitive types (e.g. `Int`, `String`, `Double`), tuple types (with arbitrary
nesting, e.g. `(String, (Int, Char), Double)`) as well as **case classes**. This is all implemented
without sacrificing performance in serialization and deserialization;
* **Scoobi applications are optimized across library boundaries**: Over time it makes sense
to partition Scoobi code into separate logical entities - into separate classes and libraries. The
advantage of Scoobi is that its staging compiler works across library boundaries. Therefore
you'll get the same Hadoop performance as if you had everything in the one file but with the
productivity gains of having modular software;
* **It's Scala**: Of course, with Scala you don't lose access to those precious Java libraries,
but you also get functional programming and concise syntax which makes writing Hadoop applications
with Scoobi very productive ... and fun!


Word count decomposed
---------------------

Let's take a step-by-step look at the simple word count example from above. The
complete application for word count looks like this:

```scala
  import com.nicta.scoobi.Scoobi._

  object WordCount extends ScoobiApp {
    val lines: DList[String] = fromTextFile(args(0))

    val counts: DList[(String, Int)] = lines.flatMap(_.split(" "))
                                            .map(word => (word, 1))
                                            .groupByKey
                                            .combine(_+_)

    persist(toTextFile(counts, args(1)))
  }
```

Our word count example is implemented by the object `WordCount`, wich extends a `ScoobiApp`. This
is a convience in Scoobi to avoid having to write a `main` function, as well as automatically
handling arguments intended for hadoop. The remaining arguments are available as `args`

Within the implementation guts, the first task is to construct a `DList` representing the data
located at the input directory. In this situation, because the input data are simple text files,
we can use the `fromTextFile` method that takes our input directory as an argument and returns
a `DList[String]` object. Here our `DList` object is a distributed collection where each
collection element is a line from the input data and is assigned to `lines`.

The second task is to compute a `DList` of word counts given all the lines of text from our
input data. This is implemented in four steps:

1. A `flatMap` is performed  on `lines`. Like `List`'s `flatMap`, a parameterizing function is
supplied which will take as its input a given line (a `String`) and will return 0 or more `String`s
as its result. In this case, that function is the method `split` which will split the input
string (a line) into a collection of words based on the occurrence of whitespace. The result of the
`flatMap` then is another `DList[String]` representing a distributed collection of words.

2. A `map` is performed on the distributed collection of words. Like `List`'s `map`, a parameterizing
function is supplied which takes as its input a given word (a `String`) and will return another value.
In this case the supplied function takes the input word and returns a pair: the word and the value 1.
The resulting object is a new distributed collection of type `DList[(String, Int)]`.

3. A `groupByKey` is performed on the `(String, Int)` distributed collection. `groupByKey` has no
direct counterpart in `List` (although there is a `groupBy` defined on `DList`s). `groupByKey` must
be called on a key-value `DList` object else the program will not type check. The effect of
`groupByKey` is to collect all distributed collection values with the same key. In this case the `DList`
object is of type `(String, Int)` so a new `DList` object will be returned of type
`(String, Iterable[Int])`. That is, the counts for the same words will be grouped together.

4. To get the total count for each word, a `combine` is performed. `combine` also has no counterpart
in `List` but its semantics are to take a `DList[(K, Iterable[V])]` and return a `DList[(K, V)]` by
reducing all the values. It is parameterized by a function of type `(V, V) => V` that must be
associative. In our case we are simply performing addition to sum all the counts.

The final task is to take the `counts` object, which represents counts for each word, and *persist* it.
In this case we will simply persist it as a text file, whose path is specified by the second command line
argument, using `toTextFile`. Note that `toTextFile` is used within `persist`. Although not demonstrated
in this example, `persist` takes a variable number of arguments, each of which specifies what `DList` is
being persisted and how.

Until `persist` is called, our application will only be running on the local client. The act of calling
`persist`, along with the `DList`(s) to be persisted, will trigger Scoobi's staging compiler to take the
sequence of `DList` transformations and turn them into one or more Hadoop MapReduce jobs. In this
example Scoobi will generate a single MapReduce job that would be executed:

* The functionality associated with the `flatMap` and `map` will become part of a *mapper* tasks;
* The transformation associated with `groupByKey` will be occur as a consequence of the *sort-and-shuffle* phase;
* The functionality of the `combine` will become part of both a *combiner* and *reducer* task.

The word count example is one of a number of examples included with Scoobi. The top level directory
[examples](https://github.com/NICTA/scoobi/tree/master/examples) contains a number of self-contained
tutorial-like examples, as well as a [guide](https://github.com/NICTA/scoobi/blob/master/examples/README.md) to
building and deploying them. This is an additional starting point for learning and using scoobi.


Loading and persisting data
---------------------------

`DList` objects are merely nodes in a graph describing a series of data computation we want to
perform. However, at some point we need to specify what the inputs and outputs to that computation
are. We have already seen this in the previous example with `fromTextFile(...)` and
`persist(toTextFile(...))`. The former is an example of *loading* data and the latter is an example
of *persisting* data.

### Loading

Most of the time when we create `DList` objects, it is the result of calling a method on another
`DList` object (e.g. `map`). *Loading*, on the other hand, is the only way to create a `DList`
object that is not based on any others. It is the means by which we associate a `DList` object with
some data files on HDFS. Scoobi provides functions to create `DList` objects associated with
text files on HDFS, which are implemented in the object
[`com.nicta.scoobi.io.text.TextInput`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.io.text.TextInput$).

The simplest, which we have seen already, is `fromTextFile`. It takes a path (globs are supported) to
text files on HDFS (or whichever file system Hadoop has been configured for) and returns a
`DList[String]` object, where each element of the distributed collection refers to one of the lines of
text from the files.

Often we are interested in loading delimited text files, for example, comma separated value (CSV) files.
In this case, we can use `fromTextFile` followed by a `map` to pull out fields of interest:

```scala
  // load CSV with schema "id,first_name,second_name,age"
  val lines: DList[String] = fromTextFile("hdfs://path/to/CVS/files/*")

  // pull out id and second_name
  val names: DList[(Int, String)] = lines map { line =>
    val fields = line.split(",")
    (fields(0).toInt, fields(2))
  }
```

This works fine, but because it's such a common task, `TextInput` also provides the function
`extractFromDelimitedTextFile` specifically for these types of field extractions:

```scala
  // load CSV and pull out id and second_name
  val names: DList[(Int, String)] = extractFromDelimitedTextFile(",", "hdfs://path/to/CVS/files/*") {
    case id :: first_name :: second_name :: age :: _ => (id.toInt, second_name)
  }
```

When using `extractFromDelimitedTextFile`, the first
argument specifies the delimiter and the second is the path. However, there is also
a second *parameter list* which is used to specify what to do with fields once they are separated out.
This is specified by supplying a *partial function* that takes a list of separated `String` fields as its input
and returns a value whose type will set the type of the resulting `DList` - i.e. a `PartialFunction[List[String], A]`
will create a `DList[A]` (where `A` is `(Int, String)` above). In this example, we use Scala's
[pattern matching](http://www.scala-lang.org/node/120) feature to *pull out* the four fields
and return the first and third.

One of the advantages of this approach is that we have at our disposal all of the Scala
pattern matching features, and because we are providing a partial function, any fields
that don't match against the supplied pattern will not be present in the returned `DList`.
This allows us implement simple filtering inline with the extraction:

```scala
  // load CSV and pull out id and second_name if first_name is "Harry"
  val names: DList[(Int, String)] = extractFromDelimitedTextFile(",", "hdfs://path/to/CSV/files/*") {
    case id :: "Harry" :: second_name :: age :: _ => (id.toInt, second_name)
  }
```

We can of course supply multiple patterns:

```scala
  // load CSV and pull out id and second_name if first_name is "Harry" or "Lucy"
  val names: DList[(Int, String)] = extractFromDelimitedTextFile(",", "hdfs://path/to/CSV/files/*") {
    case id :: "Harry" :: second_name :: age :: _ => (id.toInt, second_name)
    case id :: "Lucy"  :: second_name :: age :: _ => (id.toInt, second_name)
  }
```

And, a more interesting example is when the value of one field influences the semantics of
another. For example:

```scala
  val thisYear: Int = ...

  // load CSV with schema "event,year,year_designation" and pull out event and how many years ago it occurred
  val yearsAgo: DList[(String, Int)] = extractFromDelimitedTextFile(",", "hdfs://path/to/CSV/files/*") {
    case event :: year :: "BC" :: _ => (event, thisYear + year.toInt - 1) // No 0 AD
    case event :: year :: "AD" :: _ => (event, thisYear - year.toInt)
  }
```

These are nice features. However, one of the problems with these examples is their conversion of
a `String` fields into an `Int`. If the field is not supplied (e.g. empty string) or the files
are simply erroneous, a run-time exception will occur when `toInt` is called. This exception will be
caught by Hadoop and likely cause the MapReduce job to fail. As a solution to this problem, `TextInput`
provides Scala [extractors](http://www.scala-lang.org/node/112) for `Int`s, `Long`s and `Double`s.
Using the `Int` extractor we can rewrite one of the above examples:

```scala
  // load CSV and pull out id and second_name
  val names: DList[(Int, String)] = extractFromDelimitedTextFile(",", "hdfs://path/to/CSV/files/*") {
    case Int(id) :: first_name :: second_name :: Int(age) :: _ => (id, second_name)
  }
```

Here, the pattern will only match if the `id` (and `age`) field(s) can be converted successfully from a
`String` to an `Int`.  If not, the pattern will not match and that line will not be extracted into the
resulting `DList`.


### Persisting

*Persisting* is the mechanism Scoobi uses for specifying that the result of executing the computational graph
associated with a `DList` object is to be associated with a particular data file on HDFS. There are
two parts to persisting:

1. Calling `persist`, which bundles all `DList` objects being persisted;
2. Specifying how each `DList` object is to be persisted.

Scoobi currently only provides one mechanism for specifying how a `DList` is to be persisted. It is
`toTextFile` and is implemented in the object
[`com.nicta.scoobi.io.text.TextOutput`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.io.text.TextOutput$).
As we have seen previously, `toTextFile` takes two arguments: the `DList` object being persisted and
the directory path to write the resulting data:

```scala
  val rankings: DList[(String, Int)] = ...

  persist(toTextFile(rankings, "hdfs://path/to/output"))
```

`persist` can of course bundle together more than one `DList`. For example:

```scala
  val rankings: DList[(String, Int)] = ...
  val rankings_reverse: DList[(Int, String)] = rankings map { swap }
  val rankings_example: DList[(Int, String)] = rankings_reverse.groupByKey.map{ case (ranking, items) => (ranking, items.head) }

  persist(toTextFile(rankings,         "hdfs://path/to/output"),
          toTextFile(rankings_reverse, "hdfs://path/to/output-reverse"),
          toTextFile(rankings_example, "hdfs://path/to/output-example"))
```

As mentioned previously, `persist` is the trigger for executing the computational graph associated
with its `DList` objects. By bundling `DList` objects together, `persist` is able to determine computations
that are shared by those outputs and ensure that they are only performed once.


Data types
----------

We've seen in many of the examples that it's possible for `DList` objects to be parameterized
by normal Scala primitive (*value*) types. Not surprisingly, Scoobi supports `DList` objects
that are parameterized by any of the Scala primitive types:

```scala
  val x: DList[Byte] = ...
  val x: DList[Char] = ...
  val x: DList[Int] = ...
  val x: DList[Long] = ...
  val x: DList[Double] = ...
```

And as we've also see, although not a primitive, Scoobi supports `DList`s of `String`s:

```scala
  val x: DList[String] = ...
```

Some of the examples also use `DList` objects that are parameterized by a pair (Scala
[`Tuple2`](http://www.scala-lang.org/api/current/scala/Tuple2.html) type).
In fact, Scoobi supports `DList` objects that are parameterized by Scala tuples up to arity 8, and in addition,
supports arbitrary nesting:

```scala
  val x: DList[(Int, String)] = ...
  val x: DList[(String, Long)] = ...
  val x: DList[(Int, String, Long)] = ...
  val x: DList[(Int, (String, String), Int, (Long, Long, Long))] = ...
  val x: DList[(Int, (String, (Long, Long)), Char)] = ...
```

Finally, Scoobi also supports `DList` objects that are parameterized by the Scala
[`Option`](http://www.scala-lang.org/api/rc/scala/Option.html)
and [`Either`](http://www.scala-lang.org/api/current/scala/Either.html)
types, which can also be combined with any of the `Tuple` and primitive types:

```scala
  val x: Option[Int] = ...
  val x: Option[String] = ...
  val x: Option[(Long, String)] = ...

  val x: Either[Int, String] = ...
  val x: Either[String, (Long, Long)] = ...
  val x: Either[Long, Either[String, Int]] = ...
  val x: Either[Int, Option[Long]] = ...
```

Notice that in all these cases, the `DList` object is parameterized by a *standard* Scala type
and not some wrapper type. This is really convenient. It means, for example, that the use of
a higher-order function like `map` can directly call any of the methods associated with those types.
In contrast, programming MapReduce jobs directly using Hadoop's API requires that all types implement the
[`Writable`](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Writable.html)
interface, resulting in the use of wrapper types such as `IntWritable` rather than just `int`.
Of course the reason for this is that `Writable` specifies methods for serialization and
deserialization of data within the Hadoop framework. However, given that `DList` objects eventually
result in code that is executed by the Hadoop framework, how is serialization and deserialization
specified?

Scoobi requires that the type parameterizing a `DList` object has an implementation of the
[`WireFormat`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.WireFormat) type class
(Scala [context bound](http://stackoverflow.com/questions/2982276/what-is-a-context-bound-in-scala)).
Thus, the `DList` class is actually specified as:

```scala
  class DList[A : WireFormat] { ... }
```

If the compiler cannot find a `WireFormat` implementation for the type parameterizing a specific
`DList` object, that code will not compile.  Implementations of `WireFormat` specify serialization
and deserialization in their `toWire` and `fromWire` methods, which end up finding their way into
`Writable`'s `write` and `readFields` methods.

To make life easy, the
[`WireFormat`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.WireFormat$) object
includes `WireFormat` implementations for the types listed above (that is why they work out of the
box). However, the real advantage of using
type classes is they allow you to extend the set of types that can be used with `DList` objects and
that set can include types that already exist, maybe even in some other compilation unit.  So long as
a type has a `WireFormat` implementation, it can parameterize a `DList`. This is extremely
useful because while, say, you can represent a lot with nested tuples, much can be gained in terms
of type safety, readability and maintenance by using custom types. For example,
say we were building an application to analyze stock ticker-data. In that situation it would be nice
to work with `DList[Tick]` objects. We can do that if we write a `WireFormat` implementation for `Tick`:

```scala
  case class Tick(val date: Int, val symbol: String, val price: Double)

  implicit def TickFmt = new WireFormat[Tick] {
    def toWire(tick: Tick, out: DataOutput) = {
      out.writeInt(tick.date)
      out.writeUTF(tick.symbol)
      out.writeDouble(tick.price)
    }
    def fromWire(in: DataInput): Tick = {
      val date = in.readInt
      val symbol = in.readUTF
      val price = in.readDouble
      Tick(date, symbol, price)
    }
    def show(tick: Tick): String = tick.toString
  }

  val ticks: DList[Tick] = ...  /* OK */
```

Then we can actually make use of the `Tick` type:

```scala
  /* Function to compute Hi and Low for a stock for a given day */
  def hilo(ts: Iterable[Tick]): (Double, Double) = {
    val start = ts.head.price
    ts.tail.foldLeft((start, start)) { case ((high, low), tick) => (max(high, tick.price), min(low, tick.price)) }
  }

  /* Group tick data by date and symbol */
  val ticks: DList[Tick] = ...
  val ticksGrouped = ticks.groupBy(t => (t.symbol, t.date))

  /* Compute highs and lows for each stock for each day */
  val highLow = ticksGrouped map { case ((symbol, date), ticks) => (symbol, date, hilo(ticks)) }
```

Notice that by using the custom type `Tick` it's obvious what fields we are using. If instead
the type of `ticks` was `DList[(Int, String, Double)]`, the code would be far less readable,
and maintenance would be more difficult if, for example, we added new fields to `Tick` or modified
the order of existing fields.

Being able to have `DList` objects of custom types is a huge productivity boost. However, there
is still the boiler-plate, mechanical work associated with the `WireFormat` implementation. To overcome this,
the `WireFormat` object also provides a utility function called `mkCaseWireFormat` that automatically
constructs a `WireFormat` for **case classes**:


```scala
  case class Tick(val date: Int, val symbol: String, val price: Double)
  implicit val tickFmt = mkCaseWireFormat(Tick)(Tick.unapply _)

  val ticks: DList[Tick] = ...  /* Still OK */
```

`mkCaseWireFormat` takes as arguments the case class's automatically generated `apply` and `unapply`
methods. The only requirement on case classes when using `mkCaseWireFormat` is that all its fields have
`WireFormat` implementations. If not, your `DList` objects won't type check. The upside to
this is that all of the types above that do have `WireFormat` implementations can be
fields in a case class when used in conjunction with `mkCaseWireFormat`:

```scala
  case class Tick(val date: Int, val symbol: String, val price: Double, val high_low: (Double, Double))
  implicit val tickFmt = mkCaseWireFormat(Tick)(Tick.unapply _)

  val ticks: DList[Tick] = ...  /* Amazingly, still OK */
```

Of course, this will also extend to other case classes as long as they have `WireFormat` implementations.
Thus, it's possible to have nested case classes that can parameterize `DList` objects:

```scala
  case class PriceAttr(val: price: Double, val high_low: (Double, Double))
  implicit val priceAttrFmt = mkCaseWireFormat(PriceAttr)(PriceAttr.unapply _)

  case class Tick(val date: Int, val symbol: String, val attr: PriceAttr)
  implicit val tickFmt = mkCaseWireFormat(Tick)(Tick.unapply _)

  val ticks: DList[Tick] = ...  /* That's right, amazingly, still OK */
```

In summary, the way data types work in Scoobi is definitely one of its killer features, basically
because they don't get in the way!


Creating a Scoobi project with sbt
----------------------------------

Scoobi projects are generally developed with [sbt](https://github.com/harrah/xsbt/wiki), and to simplify
the task of building and packaging a project for running on Hadoop, it's really handy to use the sbt plugin
[sbt-scoobi](https://github.com/NICTA/sbt-scoobi). Here are a few steps for creating a new project:

Create a new Scoobi application and add some code:

```
    $ mkdir my-app
    $ cd my-app
    $ mkdir -p src/main/scala
    $ vi src/main/scala/MyApp.scala
```

To use the sbt-scoobi plugin we need to include a `project/project/scoobi.scala` file with the following contents:

```scala
    import sbt._

    object Plugins extends Build {
      lazy val root = Project("root", file(".")) dependsOn(
        uri("git://github.com/NICTA/sbt-scoobi.git#master")
      )
    }
```

And, we can add a pretty standard `build.sbt` that has a dependency on Scoobi:

```scala
    name := "MyApp"

    version := "0.1"

    scalaVersion := "2.9.1"

    libraryDependencies += "com.nicta" %% "scoobi" % "0.3.0" % "provided"
```

The `provided` is added to the `scoobi` dependency to let sbt know that Scoobi
is provided by the sbt-plugin when it packages everything in a jar. If you
don't included this `provided` nothing bad will happen, but the jar will contain
some Scoobi dependencies that are not strictly required.

We can now use sbt to easily build and package our application into a self-contained executable
jar to feed directly into Hadoop:

```
    $ sbt package-hadoop
    $ hadoop jar ./target/MyApp-app-hadoop-0.1.jar <args>
```

Note that there appears to be a OSX-specific [issue](https://github.com/NICTA/scoobi/issues/1)
associated with calling `hadoop` in this manner requiring the jar to be added to `HADOOP_CLASSPATH`
and then `hadoop` being given the correct object to run. e.g.:

```
    $ export HADOOP_CLASSPATH=$PWD/target/Scoobi_Word_Count-hadoop-0.1.jar
    $ hadoop WordCount inputFile/to/wordcount nonexistent/outputdir
```


Issues, questions and contributions
-----------------------------------

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


Release notes
-------------------
[Changes in 0.3.0](https://github.com/nicta/scoobi/blob/master/CHANGES.md)

### Next Milestone
[0.4.0 Open issues](https://github.com/NICTA/scoobi/issues?milestone=2)

[0.4.0 Closed issues](https://github.com/NICTA/scoobi/issues?milestone=2&state=closed)

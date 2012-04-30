package com.nicta.scoobi.guide

import org.specs2.Specification

class Overview extends Specification { def is =
                                                                                                                        """
Scoobi is centered around the idea of a **distributed collection**, which is implemented by the [`DList`](http://nicta.github.com/scoobi/master/index.html#com.nicta.scoobi.DList) (*distributed list*) class.  In a lot of ways, `DList` objects are similar to normal [Scala `List`](http://www.scala-lang.org/api/current/scala/collection/immutable/List.html) objects: they are parameterized by a type and they provide methods that can be used to produce new `DList` objects, often parameterized by higher-order functions. For example:

      // Converting a List[Int] to a List[String] keeping only evens
      val stringList = intList filter { _ % 2 == 0 } map { _.toString }

      // Converting a DList[Int] to a DList[String] keeping only evens
      val stringDList = intDList filter { _ % 2 == 0 } map { _.toString }

However, unlike a Scala `List` object, the contents of `DList` objects are not stored on the JVM heap but stored in HDFS. Secondly, calling `DList` methods will not immediately result in data being generated in HDFS. This is because, behind the scenes, Scoobi implements a *staging compiler*. The purpose of `DList` methods are to construct a *graph* of data transformations. Then, the act of *persisting* a `DList` triggers the compilation of the graph into one or more MapReduce jobs and their execution.

So, `DList` objects essentially provide two abstractions:

1. The contents of a `DList` object abstracts the storage of data and files in HDFS;
2. Calling methods on `DList` objects to transform and manipulate them abstracts the *mapper*, *combiner*, *reducer* and *sort-and-shuffle* phases of MapReduce.

So, what are some of the advantages of using Scoobi?

* **The collections abstraction implemented by `DList` is a familiar one**: The methods for the `DList` class have been designed to be the same or as similar to those implemented in the standard Scala collections. There aren't as many methods, but if you grok the semantics of Scala collections, you shouldn't have too much trouble getting up to speed with Scoobi

 * **The `DList` class is strongly typed**: Like the Scala collections, the `DList` interface is strongly typed so that more errors are caught at compile time. This is a major improvement over standard Hadoop MapReduce where type-based run-time errors often occur

 * **The `DList` class can be easily parameterized on rich data types**: Unlike Hadoop MapReduce, which requires that you go off implementing a myriad of classes that implement the `Writable` interface, Scoobi allows `DList` objects to be parameterized by normal Scala types. This includes the primitive types (e.g. `Int`, `String`, `Double`), tuple types (with arbitrary nesting, e.g. `(String, (Int, Char), Double)`) as well as **case classes**. This is all implemented without sacrificing performance in serialization and deserialization

 * **Scoobi applications are optimized across library boundaries**: Over time it makes sense to partition Scoobi code into separate logical entities - into separate classes and libraries. The advantage of Scoobi is that its staging compiler works across library boundaries. Therefore you'll get the same Hadoop performance as if you had everything in the one file but with the productivity gains of having modular software

 * **It's Scala**: Of course, with Scala you don't lose access to those precious Java libraries, but you also get functional programming and concise syntax which makes writing Hadoop applications with Scoobi very productive ... and fun!

Word count decomposed
---------------------

Let's take a step-by-step look at the simple word count example from above. The complete application for word count looks like this:

        import com.nicta.scoobi.Scoobi._

        object WordCount extends ScoobiApp {
          val lines: DList[String] = fromTextFile(args(0))

          val counts: DList[(String, Int)] = lines.flatMap(_.split(" "))
                                            .map(word => (word, 1))
                                            .groupByKey
                                            .combine(_+_)

          persist(toTextFile(counts, args(1)))
        }

Our word count example is implemented by the object `WordCount`, wich extends a `ScoobiApp`. This is a convenience in Scoobi to avoid having to write a `main` function, as well as automatically handling arguments intended for hadoop. The remaining arguments are available as `args`.

Within the implementation guts, the first task is to construct a `DList` representing the data located at the input directory. In this situation, because the input data are simple text files, we can use the `fromTextFile` method that takes our input directory as an argument and returns a `DList[String]` object. Here our `DList` object is a distributed collection where each collection element is a line from the input data and is assigned to `lines`.

The second task is to compute a `DList` of word counts given all the lines of text from our input data. This is implemented in four steps:

1. A `flatMap` is performed  on `lines`. Like `List`'s `flatMap`, a parameterizing function is supplied which will take as its input a given line (a `String`) and will return 0 or more `String`s as its result. In this case, that function is the method `split` which will split the input string (a line) into a collection of words based on the occurrence of whitespace. The result of the `flatMap` then is another `DList[String]` representing a distributed collection of words.

2. A `map` is performed on the distributed collection of words. Like `List`'s `map`, a parameterizing function is supplied which takes as its input a given word (a `String`) and will return another value.
In this case the supplied function takes the input word and returns a pair: the word and the value 1. The resulting object is a new distributed collection of type `DList[(String, Int)]`.

3. A `groupByKey` is performed on the `(String, Int)` distributed collection. `groupByKey` has no direct counterpart in `List` (although there is a `groupBy` defined on `DList`s). `groupByKey` must
be called on a key-value `DList` object else the program will not type check. The effect of `groupByKey` is to collect all distributed collection values with the same key. In this case the `DList`
object is of type `(String, Int)` so a new `DList` object will be returned of type `(String, Iterable[Int])`. That is, the counts for the same words will be grouped together.

4. To get the total count for each word, a `combine` is performed. `combine` also has no counterpart in `List` but its semantics are to take a `DList[(K, Iterable[V])]` and return a `DList[(K, V)]` by
reducing all the values. It is parameterized by a function of type `(V, V) => V` that must be associative. In our case we are simply performing addition to sum all the counts.

The final task is to take the `counts` object, which represents counts for each word, and *persist* it. In this case we will simply persist it as a text file, whose path is specified by the second command line
argument, using `toTextFile`. Note that `toTextFile` is used within `persist`. Although not demonstrated in this example, `persist` takes a variable number of arguments, each of which specifies what `DList` is
being persisted and how.

Until `persist` is called, our application will only be running on the local client. The act of calling `persist`, along with the `DList`(s) to be persisted, will trigger Scoobi's staging compiler to take the
sequence of `DList` transformations and turn them into one or more Hadoop MapReduce jobs. In this example Scoobi will generate a single MapReduce job that would be executed:

* The functionality associated with the `flatMap` and `map` will become part of a *mapper* tasks
* The transformation associated with `groupByKey` will be occur as a consequence of the *sort-and-shuffle* phase
* The functionality of the `combine` will become part of both a *combiner* and *reducer* task

The word count example is one of a number of examples included with Scoobi. The top level directory [examples](https://github.com/NICTA/scoobi/tree/master/examples) contains a number of self-contained
tutorial-like examples, as well as a [guide](https://github.com/NICTA/scoobi/blob/master/examples/README.md) to building and deploying them. This is an additional starting point for learning and using scoobi.
                                                                                                                        """^
                                                                                                                        end
}

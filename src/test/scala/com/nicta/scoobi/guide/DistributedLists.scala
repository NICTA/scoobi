/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package guide

class DistributedLists extends ScoobiPage { def is = "Distributed Lists".title ^
  """

### Introduction

Scoobi is centered around the idea of a **distributed collection**, which is implemented by the [`DList`](${SCOOBI_API_PAGE}#com.nicta.scoobi.DList) (*distributed list*) class.  In a lot of ways, `DList` objects are similar to normal [Scala `List`](http://www.scala-lang.org/api/current/scala/collection/immutable/List.html) objects: they are parameterised by a type and they provide methods that can be used to produce new `DList` objects, often parameterised by higher-order functions. For example:

```scala
// Converting a List[Int] to a List[String] keeping only evens
val stringList = intList filter { _ % 2 == 0 } map { _.toString }

// Converting a DList[Int] to a DList[String] keeping only evens
val stringDList = intDList filter { _ % 2 == 0 } map { _.toString }
```

However, unlike a Scala `List` object, the contents of `DList` objects are not stored on the JVM heap but stored elsewhere, typically HDFS. Secondly, calling `DList` methods will not immediately result in data being generated in HDFS. This is because, behind the scenes, Scoobi implements a *staging compiler*. The purpose of `DList` methods are to construct a *graph* of data transformations. Then, the act of *persisting* a `DList` triggers the compilation of the graph into one or more MapReduce jobs and their execution.


In summary, `DList` objects essentially provide two abstractions:

1. The contents of a `DList` object abstracts the storage of data and files in HDFS;
2. Calling methods on `DList` objects to transform and manipulate them abstracts the *mapper*, *combiner*, *reducer* and *sort-and-shuffle* phases of MapReduce.


### Word count decomposed

Let's take a step-by-step look at the simple word count example from above. The complete application for word count looks like this:

```scala
import com.nicta.scoobi.Scoobi._

object WordCount extends ScoobiApp {
  def run() {
    val lines: DList[String] = fromTextFile(args(0))

    val counts: DList[(String, Int)] = lines.flatMap(_.split(" "))
                                            .map(word => (word, 1))
                                            .groupByKey
                                            .combine(_+_)

    persist(counts.toTextFile(args(1)))
  }
}
```

Our word count example is implemented by the object `WordCount`, wich extends a `ScoobiApp`. This is a convience in Scoobi to avoid having to create configuration objects, as well as automatically handling arguments intended for hadoop. The remaining arguments are available as `args`

Within the implementation guts, the first task is to construct a `DList` representing the data located at the input directory. In this situation, because the input data are simple text files, we can use the `fromTextFile` method that takes our input directory as an argument and returns a `DList[String]` object. Here our `DList` object is a distributed collection where each collection element is a line from the input data and is assigned to `lines`.

The second task is to compute a `DList` of word counts given all the lines of text from our input data. This is implemented in four steps:

1. A `flatMap` is performed  on `lines`. Like `List`'s `flatMap`, a parameterizing function is supplied which will take as its input a given line (a `String`) and will return 0 or more `String`s as its result. In this case, that function is the method `split` which will split the input string (a line) into a collection of words based on the occurrence of whitespace. The result of the `flatMap` then is another `DList[String]` representing a distributed collection of words.

2. A `map` is performed on the distributed collection of words. Like `List`'s `map`, a parameterizing function is supplied which takes as its input a given word (a `String`) and will return another value.  In this case the supplied function takes the input word and returns a pair: the word and the value 1.  The resulting object is a new distributed collection of type `DList[(String, Int)]`.

3. A `groupByKey` is performed on the `(String, Int)` distributed collection. `groupByKey` has no direct counterpart in `List` (although there is a `groupBy` defined on `DList`s). `groupByKey` must be called on a key-value `DList` object else the program will not type check. The effect of `groupByKey` is to collect all distributed collection values with the same key. In this case the `DList` object is of type `(String, Int)` so a new `DList` object will be returned of type `(String, Iterable[Int])`. That is, the counts for the same words will be grouped together.

4. To get the total count for each word, a `combine` is performed. `combine` also has no counterpart in `List` but its semantics are to take a `DList[(K, Iterable[V])]` and return a `DList[(K, V)]` by reducing all the values. It is parameterised by a function of type `(V, V) => V` that must be associative. In our case we are simply performing addition to sum all the counts.

The final task is to take the `counts` object, which represents counts for each word, and *persist* it.  In this case we will simply persist it as a text file, whose path is specified by the second command line argument, using `toTextFile`.

Until `persist` is called, our application will only be running on the local client. The act of calling `persist`, along with the `DList`(s) to be persisted, will trigger Scoobi's staging compiler to take the sequence of `DList` transformations and turn them into one or more Hadoop MapReduce jobs. In this example Scoobi will generate a single MapReduce job that would be executed:

* The functionality associated with the `flatMap` and `map` will become part of a *mapper* tasks;
* The transformation associated with `groupByKey` will be occur as a consequence of the *sort-and-shuffle* phase;
* The functionality of the `combine` will become part of both a *combiner* and *reducer* task.

The word count example is one of a number of examples included with Scoobi. The top level directory [examples](https://github.com/NICTA/scoobi/tree/${SCOOBI_BRANCH}/examples) contains a number of self-contained tutorial-like examples, as well as a [guide](https://github.com/NICTA/scoobi/blob/${SCOOBI_BRANCH}/examples/README.md) to building and deploying them. This is an additional starting point for learning and using scoobi.

### Parallel operations

We have already seen a number of `DList` methods - `map`, `flatMap`. These methods are parallel operations in that they are performed in parallel by Hadoop across the disributed data set. The `DList` trait implements parallel operations for many of the methods that you would find in the standard Scala collections:

 * `map`
 * `flatMap`
 * `filter`
 * `filterNot`
 * `collect`
 * `partition`
 * `flatten`
 * `distinct`
 * `++`

All of these methods are built upon the primitive parallel operation `parallelDo`.  Unlike the other `DList` methods, `parallelDo` provides a less *functional* interface and requires the user to implement a [`DoFn`](${SCOOBI_API_PAGE}#com.nicta.scoobi.DoFn) object:

```scala
def parallelDo[B](dofn: DoFn[A, B]): DList[B]

trait DoFn[A, B] {
  def setup(): Unit
  def process(input: A, emitter: Emitter[B]): Unit
  def cleanup(emitter: Emitter[B]): Unit
}
```

Because the `DoFn` object has an interface that is closely aligned to the Hadoop mapper and reducer task APIs it allows greater flexibility and control beyond what the collections-style APIs provide. For example, a `DoFn` object can maintain state where the other APIs can not:

```scala
// Fuzzy top 10 - each mapper task will only emit the top 10 integers it processes
val ints: DList[Int] = ...
val top10ints: DList[Int] = ints.parallelDo(new DoFn[Int, Int] {
  val top = scala.collection.mutable.Set[Int].empty
  def setup() {}
  def process(input: Int, emitter: Emitter[Int]) {
    if (top.size < 10) {
      top += input
    }
    else if (input > top.min) {
      top -= top.min
      top += input
    }
  }
  def cleanup(emitter: Emitter[Int]) { top foreach { emitter.emit(_) } }
})

```

Whilst the `parallelDo` and `DoFn` APIs provide greater flexibility, it is best practice to use the collections-style APIs where possible.


### Grouping

We have already seen the `groupByKey` method. `DList` also has a `groupBy` method that allows you to specicfy how the key is determined:

```scala
case class Person(name: String, age: Int)
val people: DList[Person] = ...
val peoplebyAge: DList[(Int, Person)] = people.groupBy(_.age)
```

The grouping methods abstract Hadoop's sort-and-shuffle phase. As such it is possible to have more control over this phase using the `Grouping` type class. This allows functionality such as secondary sorting to be implemented. Refer to the Grouping section for a more detailed explanation.


### Combining

The `combine` method is Scoobi's abstraction of Hadoop's *combiner* functionality. For best results, `combine` should be
called immediately after a `groupBy` or `groupByKey` method, as in the Word Count example.

The `combine` method accepts an argument of the type `Reduction[V]` where `V` represents the type of value in the key/value pair. A reduction denotes a binary operation on a closed set (`(V, V) => V`). The `Reduction` class and object provide combinators for constructing reductions from existing ones.

For example, to obtain a reduction that performs append on a list of strings (`Reduction[List[String]]`):

    val red: Reduction[List[String]]
      = Reduction.string.list

Another example performs a reduction on a pair of integer addition and string append, then appending that pair in `Option`:

   val red: Reduction[Option[(Int, String)]] =
      = (Reduction.Sum.int zip Reduction.string).option

The API for `Reduction` provides many more functions for combining and building reduction values. API documentation is provided for each.

The acceptance tests for `Reduction` provide more usage examples with documentation (`com.nicta.scoobi.acceptance.ReductionSpec`). The automated tests for `Reduction` provide a specification for the algebraic program properties that reductions satisfy (`com.nicta.scoobi.core.ReductionSpec`).

### Creating and persisting DLists

`DList` objects are merely nodes in a graph describing a series of data transformations we want to perform. However, at some point we need to specify what the inputs and outputs to that computation are. We have already seen this in the previous example with `fromTextFile(...)` and `persist(toTextFile(...))`. The former is an example of *loading* data and the latter is an example of *persisting* data.

There are many ways of creating a new `DList` by *loading* data. Data can be loaded from files or various formats (e.g. text, sequence files, Avro files). Similarly, there are many ways in which a `DList` can be persisted. The Input and Output section provides a complete listing of all loading and persisting mechanisms.

There are two parts to persisting:

1. Specifying how the `DList` object is to be persisted;
2. Calling `persist`, which triggers the evaluation of the `DList` objects computation.

For example, to persist a `DList` to a text file we could write:

```scala
val rankings: DList[(String, Int)] = ...
persist(toTextFile(rankings, "hdfs://path/to/output"))
```

It is important to note that a call to `persist` is the mechansim for triggering the computation of a `DList` and all its dependencies. Until `persist` is called, a `DList` is simply a specification for some distributed computation. `persist` can of course bundle together multiple `DLists` allowing it to be aware of any shared computations:

```scala
val rankings: DList[(String, Int)] = ...
val rankings_reverse: DList[(Int, String)] = rankings.map(_.swap)
val rankings_example: DList[(Int, String)] = rankings_reverse.groupByKey.map{ case (ranking, items) => (ranking, items.head) }

persist(toTextFile(rankings,         "hdfs://path/to/output"),
        toTextFile(rankings_reverse, "hdfs://path/to/output-reverse"),
        toTextFile(rankings_example, "hdfs://path/to/output-example"))
```

  """
}

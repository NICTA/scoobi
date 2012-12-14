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

class DistributedObjects extends ScoobiPage { def is = "Distributed Objects".title ^
  """

### Introduction

The distributed list abstraction is very useful for specifying operations that transform large data sets into another large data set. Because these data sets are so large they typically reside on systems such as HDFS. There are other operations, however, which involve transforming a large data set into a small data set that is able to fit within the memory of a single machine. Examples of such use case are:

* From a collection of numbers, computing the average value;
* From a collection of entity-score pairs, determining the ten entities with the highest scores;
* From a corpus of documents, compute word frequencies for words present in a 1000-word dictionary.

Scoobi provides the distributed object abstraction as a solution for these cases. Like distributed lists, distributed objects are delayed computations. However, whereas a distributed list abstracts a very large data set in, say, HDFS, a distributed object simply abstract an in-memory value, which is typically the result of a distributed list (MapReduce) operation. Distributed objects are described by the [`DObject`](${SCOOBI_API_PAGE}#com.nicta.scoobi.DObject) trait.


### Materializing

After computing a `DList` on Hadoop, it is often desirable to be able to work with its contents as an ordinary Scala collection on the client. For example, it can be common to perform some operations that results in a `DList` with relatively few entires, *relative* meaning the entries could fit into the memory of a single machine.

To bring the contents of such a `DList` on to the client, you can use the `materialise` method:

```scala
val xs: DList[Int] = ...
val ys: DObject[Iterable[Int]] = persist(xs.materialise)
// do something on the client
val zs: Iterable[Int] = ys map { .... }
```

The `DList` `materialise` method applied to a `DList[A]` will return a `DObject[Iterable[A]]`. Persisting the `DObject` will force it, and its dependencies, to be computed, and returin an `Iterable[A]`. The `Iterable` can then be used as the basis for client-based computations.


### Reduction operations

In addition to `materialise`, the `DList` trait implements a series or *reduction operators* which all return `DObjects`. For example, `sum` can be applied to any `DList` of `Numeric` types and returns a `DObject` - a delayed computation of the sum of all elements in the `DList`:

```scala
val floats: DList[Float] = ...
val total = persist(floats.sum)
```

The complete list of reduction operators are:

 * reduce
 * product
 * sum
 * length
 * size
 * count
 * max
 * maxBy
 * min
 * minBy


### Working with Distributed Objects

So far we have only seen examples of how `DObjects` are created and how we can get at their inner values by persisting them. Here, the `DObject` type is a way of differentiating types of values: large-scale versus in-memory. However, this alone does not make `DObjects` particularly useful - the same functionality could be implemented using `DLists` alone.

The real advantage of `DObjects` is they provide a bridge between Hadoop-side computations and client-side computations. Say, for example, we want to subtract the minimum value from every element of a `DList[Int]`. We now know how we can compute the minimum:

```scala
val ints: DList[Int] = ...
val minimum: DObject[Int] = ints.min
```

But how can we use the `minimum` value within a `map` on `ints`?

```scala
val normalised: DList[Int] = ints map { i => i - /* minimum */ }
```

We can't use `minimum` directly inside the `map` method because it's a `DObject`. However, what we want to specify is that the *computation* represented by `normalised` is dependent on the computation of both `ints` and `minimum`. We can specify this using the `join` method:

```scala
val normalised: DList[Int] = (minimum join ints) map { case (m, i) => i - m }
```

`join` logically replicates a `DObject` value to every entry of the `DList` it is joined with.d Physically, Scoobi will push the contents of the `DObject` from the client to Hadoop's mapper and reduer tasks.

What if we wanted to apply a function `f` to the minimum value before subtracting it from every `DList` element. We could apply `f` within the `map` method:

```scala
val f: Int => Int = ...
val normalised: DList[Int] = (minimum join ints) map { case (m, i) => i - f(m) }
```

This will execute `f` on Hadoop for every element of the `DList`. This may not be a problem but what if `f` is expensive or needs access, say, to the client's local file system. In that case, it would be useful to apply `f` on the client. We can do that using the `DObject` `map` method:


```scala
val f: Int => Int = ...
val minMod: DObject[Int] = minimum.map(f)
val normalised: DList[Int] = (minMod join ints) map { case (m, i) => i - f(m) }
```

This will now apply `f` to the mimimum value as a client-side computation. Note that we can't apply `f` to `minimum` directly because it's of type `DObject[Int]`, not `Int`. The `map` method, however, is able to apply functions to the values wrapped within.

Looking at this example in total, we can see how `DObjects` are used to bridge between Hadoop and client-side computations in a way that allows Scoobi to be aware of all dependencies:

```scala
val f: Int => Int = ...
val ints: DList[Int] = ...

/* Hadoop-side computation */
val minimum: DObject[Int] = ints.min

/* Client-side computation */
val minMod: DObject[Int] = minimum.map(f)

/* Hadoop-side computations */
val normalised: DList[Int] = (minMod join ints) map { case (m, i) => i - f(m) }
```

Finally, multiple `DObjects` can be combined together as a single `DObject` by tupling:

```scala
val a: DObject[A] = ...
val b: DObject[B] = ...
val ab: DObject[(A, B)] = (a, b)
```

This makes it possible to make a `DList` computation dependent on multiple `DObjects`:

```scala
val ints: DList[Int] = ...
val lower: DObject[Int] = ...
val upper: DObject[Int] = ...
val bounded: DList[Int] = ((lower, upper) join ints) filter { case ((l, u), i) => i > l && i < u }
```

### Example

A good way of illustrating the use of distributed objects is to contrast it with what is difficult to do with distributed lists alone. For example, how might we implement the following with Scoobi's distributed list abstraction: from a large collection of integers, filter out those that are less than the average? Logically, the steps would look something like the following:

 1. Create a `DList[Int]` representing the collection of integers (e.g. maybe read in the integers from a series of text files);
 2. Compute the average `Int` value accross the entire `DList[Int]`;
 3. Filter the `DList[Int]` based on whether each integer value is less than the computed average.

With the distributed list abstraction we have described thus far, it would not be possible to implement the above easily. You would need one Scoobi job that computed and persisted to file the sum of all the integers as well as the total number of integers. Then, the application would need to read those two files in order to extract the sum and the total, from which the average could be computed. Finally, another Scoobi job would be needed to filter the original DList with the computed average.

So, it's possible but not convenient. In order for it to be convenient, there are problems with both steps 2 and 3 above that need to be overcome:

 * How do we represent the average value of a `DList[Int]`? Using a `DList[Int]` to represent the average is not the best solution as it will always be of length 1;
 * Even if we did represent the average as a `DList[Int]` of legth 1, how do we *inject* the value into the body of the filter predicate that is applied to the original `DList`?

The distributed object abstraction allows us to more easily solve this problem:

```scala
val ints: DList[Int] = fromTextFile("hdfs://all/my/integers") collect { case AnInt(i) => i }
val total: DObject[Int] = ints.sum
val num: DObject[Int] = ints.size
val average: DObject[Float] = (total, num) map { case (t, s) => t / s }
val bigger: DList[Int] = (average join ints) filter { case (a, i) => i > a } .values
persist(toTextFile(bigger, "hdfs://all/my/big-integers"))
```

In this example, we first encounter the use of `DObject` as the return value of the `DList` `sum` method. Rather than returning a `DList[Int]` with a single entry, `sum` instead returns a `DObject[Int]`. This `DObject` represents a delayed computation that if executed would calculate the sum of the intergers in `ints`. Similarly for `num` which is the result of the `DList` method `size`.

The average interger value can be calculated using `total` and `num`, however, it too is a `DObject` which means it's also a delayed computation. By pairing `total` and `size, we create a `DObject[(Int, Int)]` object that can be mapped over to compute `average`. Finally, in order to get access to `average` when we filter the orignal `DList`, we *join* `average` (`DObject[Float]) with `ints` (`DList[Int]`) which results in a new `DList` where every element is paired with the `DObject`. In this case, `filter` will operate on a `DList[(Float, Int)]`.


### Persisting Distributed Objects

We've seen previously that the way to "get inside" a `DObject` is to persist - that is, compute it:

```scala
val x: DObject[A] = ...
val y: A = persist(x)
```

Like `DLists`, it's also possible to persist multiple `DObjects` at a time. This similarly has the advantage of jointly optimising the dependency graph formed by all `DObjects`.

```scala
val a: DObject[A] = ...
val b: DObject[B] = ...
val c: DObject[C] = ...
val (aR: A, bR: B, cR: C) = persist(a, b, c)
```

Finally, it's also possible to persist both `DLists` and `DObjects` together:

```scala
val a: DObject[A] = ...
val bs: DList[B] = ...
val c: DObject[C] = ...
val ds: DList[D] = ...
val (aR: A, _, cR: C, _) = persist(a, toTextFile(bs, "hdfs://..."), c, toTextFile(ds, "hfds://..."))
```

Note that persisting a `DList` does not return a value, hence the underscore.

  """

}

//- Do we need a "stats" library for Scoobi - a peer to the "join" library?
//  - general statistics metrics over large data sets:
//    - average: mean + median
//    - standard deviation
//    - variance
//    - etc
